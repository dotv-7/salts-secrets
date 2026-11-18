"""Local stand-in for Hermes' OpenAI-compatible API, for previewing the demo chat without a model.
Owner (/v1) answers from the owner home; guest (/p/<profile>/v1) answers ONLY from the public projection.
An owner message containing ``rm -rf`` is held for consent like a real dangerous command, so the
approval card can be tried: the stream sends ``approval.request`` and waits for /v1/runs/<id>/approval."""

import asyncio
import json
import os
import secrets
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from salts_secrets import context as ctx

OWNER_HOME = Path(os.environ["SALT_OWNER_HOME"])
PUBLIC_HOME = Path(os.environ["SALT_PUBLIC_HOME"])
KEYS = {"owner": os.environ["SALT_OWNER_KEY"], "guest": os.environ["SALT_PUBLIC_KEY"]}
app = FastAPI()
WAITING: dict[str, asyncio.Future] = {}  # run id -> the owner's answer to a held command


def answer(role: str, q: str) -> str:
    if role == "owner":
        mem = (OWNER_HOME / "memories" / "MEMORY.md").read_text(encoding="utf-8").split(ctx.ENTRY_DELIMITER)
        return ("**[Owner 会话 · 演示后端]** 你的完整 memory 我都能看到：\n\n" +
                "\n".join(f"- {m.strip()}" for m in mem) + f"\n\n| 来源 | 条数 |\n|---|---|\n| MEMORY.md | {len(mem)} |")
    try:
        proj = json.loads((PUBLIC_HOME / ctx.PROJECTION_FILE).read_text(encoding="utf-8"))
    except OSError:
        proj = {"facts": [], "documents": []}
    facts = proj.get("facts") or []
    docs = proj.get("documents") or []
    if any(k in q for k in ("memory", "秘密", "隐私", "system prompt", "MEMORY.md")):
        return "这是访客会话，我拿不到 Owner 的私密 memory，也没有读文件或终端的工具。我只能分享他公开的信息。"
    body = "\n".join(f"- {f}" for f in facts) or "- （Owner 还没有公开任何内容）"
    titles = "\n".join(f"- {d['title']}" for d in docs)
    return ("**[访客会话 · 演示后端]** 我只知道 Owner 公开的这些：\n\n" + body +
            (f"\n\n公开文档：\n{titles}" if titles else "") + "\n\n```python\nprint('hello from guest')\n```")


async def handle(request: Request, role: str):
    if request.headers.get("authorization") != f"Bearer {KEYS[role]}":
        raise HTTPException(401, "bad key")
    body = await request.json()
    q = body["messages"][-1]["content"]

    async def gen():
        text = answer(role, q)
        if role == "owner" and isinstance(q, str) and "rm -rf" in q:
            run_id = "chatcmpl-" + secrets.token_hex(8)
            WAITING[run_id] = asyncio.get_running_loop().create_future()
            event = {"event": "approval.request", "run_id": run_id, "request_id": secrets.token_hex(8),
                     "execution_generation": 1, "command": "rm -rf ./demo-scratch",
                     "description": "delete in a project path", "choices": ["once", "session", "always", "deny"]}
            yield "event: approval.request\ndata: " + json.dumps(event) + "\n\n"
            try:
                choice = await asyncio.wait_for(WAITING[run_id], 120)
            except TimeoutError:
                choice = "timeout"
            finally:
                WAITING.pop(run_id, None)
            text = ("**[演示后端]** 命令没有执行：" + ("你拒绝了。" if choice == "deny" else "等待批准超时。")
                    if choice in ("deny", "timeout") else f"**[演示后端]** 已批准（{choice}），假装执行完了。")
        for i in range(0, len(text), 3):
            chunk = {"choices": [{"delta": {"content": text[i:i + 3]}}]}
            yield "data: " + json.dumps(chunk, ensure_ascii=False) + "\n\n"
            await asyncio.sleep(0.02)
        yield "data: [DONE]\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")


@app.post("/v1/chat/completions")
async def owner(request: Request):
    return await handle(request, "owner")


@app.post("/p/{profile}/v1/chat/completions")
async def guest(profile: str, request: Request):
    return await handle(request, "guest")


@app.post("/v1/runs/{run_id}/approval")
async def approval(run_id: str, request: Request):
    if request.headers.get("authorization") != f"Bearer {KEYS['owner']}":
        raise HTTPException(401, "bad key")
    waiting = WAITING.get(run_id)
    if waiting is None or waiting.done():
        raise HTTPException(409, "approval_not_pending")
    waiting.set_result(str((await request.json()).get("choice")))
    return {"run_id": run_id, "status": "resolved"}
