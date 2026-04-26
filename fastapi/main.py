import asyncio
import json
import os
import sqlite3
import uuid
from datetime import datetime, date, timezone
from pathlib import Path

import frontmatter
import httpx
from fastapi import BackgroundTasks, FastAPI
from pydantic import BaseModel

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
# Job processing
# ---------------------------------------------------------------------------

async def _run_claude(url: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        "claude", "--dangerously-skip-permissions", "-p", f"/save-url {url}",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(VAULT_PATH),
    )
    stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=120)
    return proc.returncode, stderr.decode()


async def process_job(job_id: str, url: str, channel_id: str, message_id: str):
    started_at = datetime.now(timezone.utc)

    # Run claude, retry once on failure
    try:
        returncode, stderr = await _run_claude(url)
        if returncode != 0:
            await asyncio.sleep(5)
            returncode, stderr = await _run_claude(url)
    except asyncio.TimeoutError:
        await _handle_failure(job_id, channel_id, "Timed out after 120s")
        return
    except Exception as e:
        await _handle_failure(job_id, channel_id, str(e))
        return

    if returncode != 0:
        await _handle_failure(job_id, channel_id, stderr[:300])
        return

    # Parse frontmatter from the newly written file
    title, category, tags, status = None, None, [], "success_partial"
    try:
        files = sorted(INBOX_PATH.glob("*.md"), key=lambda f: f.stat().st_mtime, reverse=True)
        if files and files[0].stat().st_mtime > started_at.timestamp():
            post = frontmatter.load(str(files[0]))
            title = post.get("title")
            category = post.get("category")
            tags = post.get("tags", [])
            status = "success"
    except Exception:
        pass

    completed_at = datetime.now(timezone.utc).isoformat()
    conn = get_db()
    conn.execute(
        "UPDATE jobs SET status=?,title=?,category=?,tags=?,completed_at=? WHERE id=?",
        (status, title, category, json.dumps(tags), completed_at, job_id),
    )
    conn.commit()
    conn.close()

    # Reply to Discord
    if status == "success":
        tags_str = "  ".join(f"`{t}`" for t in tags) if tags else ""
        content = (
            f"✅ **Note added to library**\n"
            f"**{title}**\n"
            f"Category: `{category}`  {tags_str}"
        )
    else:
        content = "✅ Note added — title unknown, check inbox"

    await _notify_discord(channel_id, content)
    await _regen_status()
    asyncio.create_task(_git_push(title or url))


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
        f"❌ Failed to save — job: `{job_id}`\n```{error[:200]}```",
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
        except Exception:
            pass  # non-critical


# ---------------------------------------------------------------------------
# bot-status.md
# ---------------------------------------------------------------------------

STATUS_ICONS = {
    "success": "✅",
    "success_partial": "⚠️",
    "failed": "❌",
    "pending": "⏳",
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
        "# Discord Bot — Job Status",
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
        category = row["category"] or "—"
        created = row["created_at"][:16]
        lines.append(
            f"| `{row['id']}` | {icon} | {title} | {category} | {created} |"
        )

    failed = [r for r in rows if r["status"] == "failed"]
    if failed:
        lines += ["", "## Failed Jobs", ""]
        for row in failed:
            lines += [
                f"### `{row['id']}` — {row['created_at'][:16]}",
                f"- URL: {row['url']}",
                f"- Error: `{row['error']}`",
                "",
            ]

    BOT_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    BOT_STATUS_PATH.write_text("\n".join(lines))


# ---------------------------------------------------------------------------
# Git push
# ---------------------------------------------------------------------------

async def _git_push(label: str):
    async def run(*args):
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        await proc.wait()
        return proc.returncode

    await run("git", "-C", str(VAULT_PATH), "add", ".")
    await run("git", "-C", str(VAULT_PATH), "commit", "-m", f"bot: save-url {label[:60]}")

    for attempt in range(3):
        rc = await run("git", "-C", str(VAULT_PATH), "push")
        if rc == 0:
            break
        await asyncio.sleep(5 * (attempt + 1))
