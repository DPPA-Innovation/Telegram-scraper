"""
telegram_scraper.py
────────────────────────────────────────────────
Scrape a Telegram group/channel you are a member of
and save messages to a CSV file.

Requirements:
    pip install telethon

Usage:
    python telegram_scraper.py
"""

import asyncio
import csv
import os
import sys
from datetime import datetime, timezone
from getpass import getpass

try:
    from telethon import TelegramClient
    from telethon.errors import SessionPasswordNeededError
    from telethon.tl.types import Channel, Chat, User
except ImportError:
    print("\n  Telethon is not installed. Run:\n")
    print("    pip install telethon\n")
    sys.exit(1)

SESSION_FILE = "tg_scraper_session"


def prompt(label, secret=False):
    val = os.environ.get(label.replace(" ", "_").upper(), "")
    if val:
        return val
    if secret:
        return getpass(f"  {label}: ")
    return input(f"  {label}: ").strip()


async def main():
    print()
    print("=" * 50)
    print("  Telegram -> CSV Scraper")
    print("=" * 50)
    print()
    print("  Get API credentials free at: https://my.telegram.org")
    print()

    api_id   = prompt("API ID (number)")
    api_hash = prompt("API Hash")

    try:
        api_id = int(api_id)
    except ValueError:
        print("  API ID must be a number.")
        sys.exit(1)

    client = TelegramClient(SESSION_FILE, api_id, api_hash)
    await client.connect()

    # -- Authentication --
    if not await client.is_user_authorized():
        print()
        phone = prompt("Your phone number (e.g. +1 555 000 0000)")
        sent  = await client.send_code_request(phone)
        print()
        code  = prompt("Verification code Telegram sent you")
        try:
            await client.sign_in(phone=phone, code=code,
                                 phone_code_hash=sent.phone_code_hash)
        except SessionPasswordNeededError:
            print()
            pw = getpass("  2FA password: ")
            await client.sign_in(password=pw)

    me = await client.get_me()
    print(f"\n  Signed in as {me.first_name}\n")

    # -- List dialogs --
    print("  Loading your groups and channels...\n")
    dialogs = await client.get_dialogs()
    groups  = [d for d in dialogs if d.is_group or d.is_channel]

    if not groups:
        print("  No groups or channels found.")
        await client.disconnect()
        sys.exit(0)

    print(f"  {'#':<4} {'Name':<40} {'Type':<10}")
    print(f"  {'─'*4} {'─'*40} {'─'*10}")
    for i, d in enumerate(groups):
        kind = "channel" if d.is_channel else "group"
        print(f"  {i+1:<4} {d.name[:40]:<40} {kind:<10}")

    print()
    choice = input("  Enter the number of the group/channel to scrape: ").strip()
    try:
        selected = groups[int(choice) - 1]
    except (ValueError, IndexError):
        print("  Invalid selection.")
        await client.disconnect()
        sys.exit(1)

    # -- Date range --
    print()
    print("  Enter a start date to fetch messages FROM.")
    print("  Format: YYYY-MM-DD  (e.g. 2022-01-01)")
    date_str = input("  Start date (or press Enter to fetch everything): ").strip()

    offset_date = None
    if date_str:
        try:
            offset_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            print(f"  Will fetch messages from {date_str} onwards.")
        except ValueError:
            print("  Invalid date format -- fetching all messages instead.")

    # -- Message limit --
    print()
    limit_str = input("  Max messages to fetch? (press Enter for NO limit): ").strip()
    limit = int(limit_str) if limit_str.isdigit() else None

    # -- Output filename --
    safe_name = "".join(c if c.isalnum() or c in "-_" else "_" for c in selected.name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_file  = f"{safe_name}_{timestamp}.csv"

    # -- Scrape --
    limit_display = str(limit) if limit else "all"
    print(f"\n  Fetching {limit_display} messages from {selected.name}...")
    if offset_date:
        print(f"  Starting from {date_str} -- may take several minutes.\n")
    else:
        print()

    entity = await client.get_entity(selected.id)
    rows   = []
    count  = 0

    async for msg in client.iter_messages(entity, limit=limit,
                                          offset_date=offset_date, reverse=True):
        if not msg.text:
            continue

        if offset_date and msg.date.replace(tzinfo=timezone.utc) < offset_date:
            continue

        sender_name     = ""
        sender_username = ""
        sender_id       = ""
        if msg.sender:
            s = msg.sender
            if isinstance(s, User):
                sender_name = ((s.first_name or "") + " " + (s.last_name or "")).strip()
                sender_username = s.username or ""
                sender_id = str(s.id)
            elif isinstance(s, (Channel, Chat)):
                sender_name = s.title or ""
                sender_id   = str(s.id)

        rows.append({
            "message_id":      msg.id,
            "date":            msg.date.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "sender_id":       sender_id,
            "sender_name":     sender_name,
            "sender_username": sender_username,
            "text":            msg.text,
            "reply_to_msg_id": msg.reply_to_msg_id or "",
            "views":           getattr(msg, "views", "") or "",
            "forwards":        getattr(msg, "forwards", "") or "",
            "replies":         (msg.replies.replies if msg.replies else "") or "",
            "has_media":       bool(msg.media),
        })
        count += 1
        if count % 500 == 0:
            last_date = rows[-1]["date"][:10]
            print(f"    Fetched {count} messages... (reached {last_date})")

    # -- Write CSV --
    if not rows:
        print("  No text messages found.")
        await client.disconnect()
        sys.exit(0)

    fieldnames = [
        "message_id", "date", "sender_id", "sender_name", "sender_username",
        "text", "reply_to_msg_id", "views", "forwards", "replies", "has_media"
    ]

    # utf-8-sig adds a BOM so Excel opens Cyrillic/Ukrainian text correctly
    with open(out_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print()
    print("=" * 50)
    print(f"  Saved {len(rows)} messages to:")
    print(f"    {os.path.abspath(out_file)}")
    print("=" * 50)
    print()

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
