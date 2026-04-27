import asyncio
import json
import logging
import os
import sqlite3
import uuid
from datetime import datetime, date, timezone
from pathlib import Path

import frontmatter
import httpx
from fastapi import BackgroundTasks, FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI()

VAULT_PATH = Path("/vault")
INBOX_PATH = VAULT_PATH / "04-resources" / "inbox"
BOT_STATUS_PATH = VAULT_PATH / "00-inbox" / "bot-status.md"
DB_PATH = "/data/jobs.db"
DISCORD_TOKEN = os.environ["DISCORD_TOKEN"]


# ---------------------------------------------------------------------------
# DB
# ---------------------------------------------------------------------------

def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id          TEXT PRIMARY KEY,
            url         TEXT NOT NULL,
            status      TEXT NOT NULL,
            channel_id  TEXT NOT NULL,
            message_id  TEXT NOT NULL,
            title       TEXT,
            category    TEXT,
            tags        TEXT,
            error       TEXT,
            created_at  TEXT NOT NULL,
            completed_at TEXT
        )
    """)
    conn.commit()
    conn.close()


@app.on_event("startup")
def startup():
    init_db()


# ---------------------------------------------------------------------------
# API
# ---------------------------------------------------------------------------

class JobRequest(BaseModel):
    url: str
    channel_id: str
    message_id: str


@app.post("/jobs", status_code=202)
async def create_job(req: JobRequest, background_tasks: BackgroundTasks):
    job_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc).isoformat()

    conn = get_db()
    conn.execute(
        "INSERT INTO jobs VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (job_id, req.url, "pending", req.channel_id, req.message_id,
         None, None, None, None, now, None),
    )
    conn.commit()
    conn.close()

    background_tasks.add_task(
        process_job, job_id, req.url, req.channel_id, req.message_id
    )
    return {"job_id": job_id}


@app.get("/jobs")
def list_jobs(limit: int = 20):
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Git helpers (all capture stderr for logging)
# ---------------------------------------------------------------------------

async def _git(*args: str) -> tuple[int, str]:
    """Run a git command against the vault, return (returncode, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        "git", "-C", str(VAULT_PATH), *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stderr.decode().strip()


async def git_pull(job_id: str) -> bool:
    rc, err = await _git("pull", "--rebase")
    if rc != 0:
        logger.error("[%s] git pull failed: %s", job_id, err)
        return False
    logger.info("[%s] git pull ok", job_id)
    return True


async def git_push(job_id: str, label: str) -> bool:
    # Stage all changes
    rc, err = await _git("add", ".")
    if rc != 0:
        logger.error("[%s] git add failed: %s", job_id, err)
        return False

    # Commit
    rc, err = await _git("commit", "-m", f"bot: save-url {label[:60]}")
    if rc != 0:
        logger.warning("[%s] git commit skipped (nothing to commit?): %s", job_id, err)
        return False
    logger.info("[%s] git commit ok", job_id)

    # Push with pull-before-retry
    for attempt in range(3):
        rc, err = await _git("pull", "--rebase")
        if rc != 0:
            logger.warning("[%s] git pull (pre-push) failed: %s", job_id, err)

        rc, err = await _git("push")
        if rc == 0:
            logger.info("[%s] git push ok (attempt %d)", job_id, attempt + 1)
            return True
        logger.warning("[%s] git push failed (attempt %d): %s", job_id, attempt + 1, err)
        await asyncio.sleep(5 * (attempt + 1))

    logger.error("[%s] git push failed after 3 attempts", job_id)
    return False


# ---------------------------------------------------------------------------
# Claude runner
# ---------------------------------------------------------------------------

async def _run_claude(url: str) -> tuple[int, str, str]:
    """Run claude save-url, return (returncode, stdout, stderr)."""
    proc = await asyncio.create_subprocess_exec(
        "claude", "--dangerously-skip-permissions", "-p", f"/save-url {url}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(VAULT_PATH),
    )
    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=180)
        return proc.returncode, stdout.decode(), stderr.decode()
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise


# ---------------------------------------------------------------------------
# Job processing — main flow
# ---------------------------------------------------------------------------

async def process_job(job_id: str, url: str, channel_id: str, message_id: str):
    started_at = datetime.now(timezone.utc)
    logger.info("[%s] starting — %s", job_id, url)

    # 1. Pull latest vault
    await git_pull(job_id)

    # 2. Run claude (retry once on failure)
    try:
        rc, stdout, stderr = await _run_claude(url)
        logger.info("[%s] claude rc=%d", job_id, rc)
        if rc != 0:
            logger.warning("[%s] claude failed, retrying — stderr: %s", job_id, stderr[:300])
            await asyncio.sleep(5)
            rc, stdout, stderr = await _run_claude(url)
            logger.info("[%s] claude retry rc=%d", job_id, rc)
    except asyncio.TimeoutError:
        logger.error("[%s] claude timed out (180s)", job_id)
        await _handle_failure(job_id, channel_id, "Timed out after 180s")
        return
    except Exception as e:
        logger.error("[%s] claude exception: %s", job_id, e)
        await _handle_failure(job_id, channel_id, str(e))
        return

    if rc != 0:
        logger.error("[%s] claude failed after retry — stderr: %s", job_id, stderr[:300])
        await _handle_failure(job_id, channel_id, stderr[:300])
        return

    # 3. Parse frontmatter from the newly created file
    title, category, tags, status = None, None, [], "success_partial"
    try:
        files = sorted(INBOX_PATH.glob("*.md"), key=lambda f: f.stat().st_mtime, reverse=True)
        if files and files[0].stat().st_mtime > started_at.timestamp():
            post = frontmatter.load(str(files[0]))
            title = post.get("title")
            category = post.get("category")
            tags = post.get("tags", [])
            status = "success"
            logger.info("[%s] parsed file: %s", job_id, files[0].name)
    except Exception as e:
        logger.warning("[%s] frontmatter parse error: %s", job_id, e)

    # 4. Update DB
    completed_at = datetime.now(timezone.utc).isoformat()
    conn = get_db()
    conn.execute(
        "UPDATE jobs SET status=?,title=?,category=?,tags=?,completed_at=? WHERE id=?",
        (status, title, category, json.dumps(tags), completed_at, job_id),
    )
    conn.commit()
    conn.close()

    # 5. Notify Discord
    if status == "success":
        tags_str = "  ".join(f"`{t}`" for t in tags) if tags else ""
        content = (
            f"\u2705 **Note added to library**\n"
            f"**{title}**\n"
            f"Category: `{category}`  {tags_str}"
        )
    else:
        content = "\u2705 Note added \u2014 title unknown, check inbox"

    logger.info("[%s] done \u2014 status=%s title=%s", job_id, status, title)
    await _notify_discord(channel_id, content)

    # 6. Update status page and push to git
    await _regen_status()
    pushed = await git_push(job_id, title or url)
    if not pushed:
        logger.warning("[%s] git push failed \u2014 changes are local only", job_id)


async def _handle_failure(job_id: str, channel_id: str, error: str):
    completed_at = datetime.now(timezone.utc).isoformat()
    conn = get_db()
    conn.execute(
        "UPDATE jobs SET status='failed',error=?,completed_at=? WHERE id=?",
        (error[:500], completed_at, job_id),
    )
    conn.commit()
    conn.close()

    await _notify_discord(
        channel_id,
        f"\u274c Failed to save \u2014 job: `{job_id}`\n```{error[:200]}```",
    )
    await _regen_status()


# ---------------------------------------------------------------------------
# Discord notification
# ---------------------------------------------------------------------------

async def _notify_discord(channel_id: str, content: str):
    async with httpx.AsyncClient() as client:
        try:
            await client.post(
                f"https://discord.com/api/v10/channels/{channel_id}/messages",
                headers={"Authorization": f"Bot {DISCORD_TOKEN}"},
                json={"content": content},
                timeout=10,
            )
        except Exception as e:
            logger.warning("discord notify failed: %s", e)


# ---------------------------------------------------------------------------
# bot-status.md
# ---------------------------------------------------------------------------

STATUS_ICONS = {
    "success": "\u2705",
    "success_partial": "\u26a0\ufe0f",
    "failed": "\u274c",
    "pending": "\u23f3",
}


async def _regen_status():
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM jobs ORDER BY created_at DESC LIMIT 20"
    ).fetchall()
    conn.close()

    updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "---",
        "title: Bot Status",
        f"updated: {date.today().isoformat()}",
        "status: system",
        "---",
        "",
        "# Discord Bot \u2014 Job Status",
        "",
        f"_Last updated: {updated}_",
        "",
        "## Recent Jobs",
        "",
        "| Job | Status | Title | Category | Created |",
        "|-----|--------|-------|----------|---------|",
    ]

    for row in rows:
        icon = STATUS_ICONS.get(row["status"], "?")
        title = (row["title"] or row["url"])[:50]
        category = row["category"] or "\u2014"
        created = row["created_at"][:16]
        lines.append(
            f"| `{row['id']}` | {icon} | {title} | {category} | {created} |"
        )

    failed = [r for r in rows if r["status"] == "failed"]
    if failed:
        lines += ["", "## Failed Jobs", ""]
        for row in failed:
            lines += [
                f"### `{row['id']}` \u2014 {row['created_at'][:16]}",
                f"- URL: {row['url']}",
                f"- Error: `{row['error']}`",
                "",
            ]

    BOT_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    BOT_STATUS_PATH.write_text("\n".join(lines))
