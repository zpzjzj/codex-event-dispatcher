import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from controller import (BusyThread, Codex, accept, database, discover_pr_thread, finish_dispatch, gh_list_pages,
                        open_pull_requests, poll_items, process_one, reconcile, recover_interrupted_jobs,
                        reserve_dispatch, session_event_state, targets)


class ControllerTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.db = database(str(Path(self.temp.name) / "state.sqlite3"))
        self.addCleanup(self.db.close)
        self.repo = "example/project"
        self.payload = {
            "action": "synchronize", "number": 281,
            "repository": {"full_name": self.repo},
            "pull_request": {"head": {"sha": "abc123"}},
        }

    def test_duplicate_delivery_is_idempotent(self):
        self.assertEqual(accept(self.db, {self.repo: self.temp.name}, "d-1", "pull_request", self.payload), 1)
        self.assertEqual(accept(self.db, {self.repo: self.temp.name}, "d-1", "pull_request", self.payload), 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)

    def test_other_repository_is_ignored(self):
        self.assertEqual(accept(self.db, {"elsewhere/repo": self.temp.name}, "d-2", "pull_request", self.payload), 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0], 0)

    def test_review_comment_routes_to_pr(self):
        self.payload["action"] = "created"
        self.assertEqual(targets("pull_request_review_comment", self.payload), [("pr", 281, "abc123")])

    def test_own_review_webhook_is_acknowledged_without_job(self):
        self.payload["action"] = "created"
        self.payload["comment"] = {"id": 123, "user": {"login": "self-user"}}
        self.assertEqual(accept(self.db, {self.repo: self.temp.name}, "own-review",
                                "pull_request_review_comment", self.payload, ["self-user"]), 0)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM deliveries").fetchone()[0], 1)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 0)

    def test_dry_run_uses_bound_thread_without_consuming_job(self):
        accept(self.db, {self.repo: self.temp.name}, "d-3", "pull_request", self.payload)
        self.db.execute("INSERT INTO bindings VALUES (?,?,?,?,?)",
                        (self.repo, "pr", 281, "thread-1", self.temp.name))
        result = process_one(self.db, {"repositories": {self.repo: self.temp.name}}, True)
        self.assertEqual(result["thread"], "thread-1")
        self.assertIn("/pull/281", result["prompt"])
        self.assertEqual(self.db.execute("SELECT state FROM jobs").fetchone()[0], "queued")

    def test_webhook_signature_and_duplicate(self):
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        cfg = Path(self.temp.name) / "config.json"
        state = Path(self.temp.name) / "http.sqlite3"
        cfg.write_text(json.dumps({"repositories": {self.repo: self.temp.name},
                                   "database": str(state), "port": port}))
        env = {**os.environ, "GITHUB_WEBHOOK_SECRET": "test-only-secret"}
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).with_name("controller.py")),
             "--config", str(cfg), "serve"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, env=env,
        )
        self.addCleanup(lambda: (process.terminate(), process.wait(timeout=5)))
        body = json.dumps(self.payload).encode()
        signature = "sha256=" + hmac.new(b"test-only-secret", body, hashlib.sha256).hexdigest()

        def post(sig):
            request = Request(f"http://127.0.0.1:{port}/github", body, method="POST", headers={
                "X-Hub-Signature-256": sig,
                "X-GitHub-Delivery": "delivery-1",
                "X-GitHub-Event": "pull_request",
                "Content-Type": "application/json",
            })
            return urlopen(request, timeout=3)

        for _ in range(40):
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=.1):
                    break
            except OSError:
                time.sleep(.05)
        with self.assertRaises(HTTPError) as rejected:
            post("sha256=wrong")
        self.assertEqual(rejected.exception.code, 401)
        rejected.exception.close()
        self.assertEqual(json.load(post(signature))["accepted_jobs"], 1)
        self.assertEqual(json.load(post(signature))["accepted_jobs"], 0)
        with database(str(state)) as verify:
            self.assertEqual(verify.execute("SELECT COUNT(*) FROM jobs").fetchone()[0], 1)

    def test_poll_baseline_and_changed_item(self):
        issue = {"number": 22, "created_at": "2026-09-20T00:00:00Z",
                 "updated_at": "2026-09-28T00:00:00Z"}
        def response(*args, **kwargs):
            return subprocess.CompletedProcess(args, 0, json.dumps([issue]), "")
        with patch("controller.subprocess.run", side_effect=response):
            cfg = {"repositories": {self.repo: self.temp.name}}
            self.assertEqual(poll_items(self.db, cfg), 0)
            issue["updated_at"] = "2099-09-28T00:00:00Z"
            self.assertEqual(poll_items(self.db, cfg), 1)
            self.assertEqual(poll_items(self.db, cfg), 0)
        job = self.db.execute("SELECT kind,action FROM jobs").fetchone()
        self.assertEqual(tuple(job), ("issue", "poll.updated"))

    def test_poll_ignores_own_pr_reply_when_head_and_content_are_unchanged(self):
        item = {"number": 286, "created_at": "2026-09-28T00:00:00Z",
                "updated_at": "2026-09-28T08:00:00Z", "pull_request": {},
                "title": "Feature", "body": "Description", "state": "open", "labels": []}
        comment = {"id": 1, "updated_at": "2099-09-28T08:49:17Z", "user": {"login": "self-user"}}
        def response(command, **kwargs):
            if command[:3] == ["gh", "pr", "view"]:
                data = {"headRefOid": "abc"}
            elif command[:2] == ["gh", "api"] and command[4].endswith("/issues"):
                data = [item]
            elif command[:2] == ["gh", "api"]:
                data = [[comment]] if "/pulls/" in command[4] else [[]]
            else:
                raise AssertionError(command)
            return subprocess.CompletedProcess(command, 0, json.dumps(data), "")
        cfg = {"repositories": {self.repo: self.temp.name}, "ignore_authors": ["self-user"]}
        self.db.execute("INSERT INTO pr_snapshots VALUES (?,?,?,?)", (self.repo, 286, "abc", "CLEAN"))
        with patch("controller.subprocess.run", side_effect=response):
            self.assertEqual(poll_items(self.db, cfg), 0)
            item["updated_at"] = comment["updated_at"]
            self.assertEqual(poll_items(self.db, cfg), 0)
            comment["user"]["login"] = "reviewer"
            item["updated_at"] = "2100-09-28T08:49:18Z"
            comment["updated_at"] = item["updated_at"]
            self.assertEqual(poll_items(self.db, cfg), 1)

    def test_reconcile_notices_conflict_checks_and_reviews_after_baseline(self):
        state = {"merge": "CLEAN", "checks": [], "reviews": []}
        def response(command, **kwargs):
            if command[:3] == ["gh", "api", "graphql"]:
                data = {"data": {"repository": {"pullRequests": {
                    "nodes": [{"number": 7, "headRefOid": "abc", "mergeStateStatus": state["merge"]}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}
            elif command[:3] == ["gh", "pr", "checks"]:
                data = state["checks"]
            elif command[:2] == ["gh", "api"] and "/reviews" in command[4]:
                data = [state["reviews"]]
            else:
                data = [[]]
            return subprocess.CompletedProcess(command, 0, json.dumps(data), "")
        with patch("controller.subprocess.run", side_effect=response):
            cfg = {"repositories": {self.repo: self.temp.name}}
            self.assertEqual(reconcile(self.db, cfg), 0)
            state["merge"] = "DIRTY"
            state["checks"] = [{"name": "build", "bucket": "fail", "state": "FAILURE", "link": "x"}]
            state["reviews"] = [{"id": 1, "updated_at": "now", "state": "CHANGES_REQUESTED"}]
            self.assertEqual(reconcile(self.db, cfg), 3)
            self.assertEqual(reconcile(self.db, cfg), 0)
            review_job = self.db.execute("SELECT source_id FROM jobs WHERE action='reviews.changed'").fetchone()
            self.assertEqual(review_job["source_id"], "1")
            state["reviews"][0]["body"] = "updated feedback"
            self.assertEqual(reconcile(self.db, cfg), 1)
            state["merge"] = "CLEAN"
            state["checks"] = []
            self.assertEqual(reconcile(self.db, cfg), 2)
            state["merge"] = "DIRTY"
            state["checks"] = [{"name": "build", "bucket": "fail", "state": "FAILURE", "link": "x"}]
            self.assertEqual(reconcile(self.db, cfg), 2)
        repeated = self.db.execute("SELECT delivery_id FROM jobs WHERE action='checks.failed'").fetchall()
        self.assertEqual(len(repeated), 2)
        self.assertNotEqual(repeated[0][0], repeated[1][0])

    def test_own_review_reply_does_not_enqueue_another_review(self):
        state = {"comments": []}
        def response(command, **kwargs):
            if command[:3] == ["gh", "api", "graphql"]:
                data = {"data": {"repository": {"pullRequests": {
                    "nodes": [{"number": 286, "headRefOid": "abc", "mergeStateStatus": "CLEAN"}],
                    "pageInfo": {"hasNextPage": False, "endCursor": None}}}}}
            elif command[:3] == ["gh", "pr", "checks"]:
                data = []
            elif command[:2] == ["gh", "api"] and "/comments" in command[4]:
                data = [state["comments"]]
            else:
                data = [[]]
            return subprocess.CompletedProcess(command, 0, json.dumps(data), "")
        cfg = {"repositories": {self.repo: self.temp.name}, "ignore_authors": ["self-user"]}
        with patch("controller.subprocess.run", side_effect=response):
            self.assertEqual(reconcile(self.db, cfg), 0)
            state["comments"].append({"id": 1, "updated_at": "2026-09-28T08:49:17Z",
                                       "user": {"login": "self-user"}, "body": "fixed"})
            self.assertEqual(reconcile(self.db, cfg), 0)
            state["comments"].append({"id": 2, "updated_at": "2026-09-28T09:00:00Z",
                                       "user": {"login": "reviewer"}, "body": "please check"})
            self.assertEqual(reconcile(self.db, cfg), 1)
            job = self.db.execute("SELECT source_id FROM jobs WHERE action='review_comments.changed'").fetchone()
            self.assertEqual(job["source_id"], "2")

    def test_open_pull_requests_reads_all_pages(self):
        pages = iter([
            {"data": {"repository": {"pullRequests": {
                "nodes": [{"number": 1, "headRefOid": "a", "mergeStateStatus": "CLEAN"}],
                "pageInfo": {"hasNextPage": True, "endCursor": "cursor-1"}}}}},
            {"data": {"repository": {"pullRequests": {
                "nodes": [{"number": 2, "headRefOid": "b", "mergeStateStatus": "DIRTY"}],
                "pageInfo": {"hasNextPage": False, "endCursor": None}}}}},
        ])
        with patch("controller.subprocess.run", side_effect=lambda *a, **k:
                   subprocess.CompletedProcess(a, 0, json.dumps(next(pages)), "")) as command:
            self.assertEqual([pr["number"] for pr in open_pull_requests(self.repo)], [1, 2])
            self.assertIn("cursor=cursor-1", command.call_args_list[1].args[0])

    def test_unreadable_external_session_defers_to_desktop(self):
        client = Codex.__new__(Codex)
        with patch.object(client, "call", side_effect=RuntimeError(
                "thread/resume: failed to deserialize stored thread item: unknown variant functionCallOutput")):
            with self.assertRaises(BusyThread):
                client.run("desktop-thread", self.temp.name, "Review event", "model")

    def test_busy_thread_does_not_block_other_jobs(self):
        accept(self.db, {self.repo: self.temp.name}, "first", "pull_request", self.payload)
        second = dict(self.payload, number=286)
        accept(self.db, {self.repo: self.temp.name}, "second", "pull_request", second)
        self.db.execute("INSERT INTO bindings VALUES (?,?,?,?,?)", (self.repo, "pr", 281, "busy", self.temp.name))
        self.db.execute("INSERT INTO bindings VALUES (?,?,?,?,?)", (self.repo, "pr", 286, "free", self.temp.name))
        cfg = {"repositories": {self.repo: self.temp.name}, "debounce_seconds": 0}
        with patch("controller.Codex") as codex, patch("controller.notify_user"):
            client = codex.return_value
            client.available_model.return_value = "test"
            client.run.side_effect = [BusyThread("busy"), ("free", "proposal")]
            self.assertEqual(process_one(self.db, cfg, False)["deferred"], "thread busy")
            self.assertEqual(self.db.execute("SELECT state FROM jobs WHERE delivery_id='first'").fetchone()[0],
                             "waiting_desktop")
            self.assertEqual(process_one(self.db, cfg, False)["thread"], "free")

    def test_newer_generic_update_does_not_hide_conflict_event(self):
        now = int(time.time()) - 60
        self.db.execute("INSERT INTO jobs(delivery_id,repo,kind,number,action,created_at) "
                        "VALUES (?,?,?,?,?,?)", ("conflict", self.repo, "pr", 281, "merge_state.DIRTY", now))
        self.db.execute("INSERT INTO jobs(delivery_id,repo,kind,number,action,created_at) "
                        "VALUES (?,?,?,?,?,?)", ("update", self.repo, "pr", 281, "poll.updated", now + 1))
        result = process_one(self.db, {"repositories": {self.repo: self.temp.name}}, True)
        self.assertEqual(result["job"], 1)
        self.assertIn("Verify the current base/head conflict", result["prompt"])

    def test_desktop_dispatch_batches_events_and_requires_ack(self):
        self.db.execute("INSERT INTO bindings VALUES (?,?,?,?,?)",
                        (self.repo, "pr", 281, "desktop-thread", self.temp.name))
        for delivery, action in (("conflict", "merge_state.DIRTY"), ("review", "review_comments.changed")):
            self.db.execute("INSERT INTO jobs(delivery_id,repo,kind,number,action,created_at,state) "
                            "VALUES (?,?,?,?,?,?,?)",
                            (delivery, self.repo, "pr", 281, action, int(time.time()), "waiting_desktop"))
        cfg = {"repositories": {self.repo: self.temp.name}, "sessions_root": self.temp.name}
        batches = reserve_dispatch(self.db, cfg)
        self.assertEqual(len(batches), 1)
        batch = batches[0]
        self.assertEqual(batch["thread_id"], "desktop-thread")
        self.assertEqual(len(batch["job_ids"]), 2)
        self.assertIn("Verify the current base/head conflict", batch["prompt"])
        self.assertIn("Inspect the exact review/comment IDs", batch["prompt"])
        self.assertEqual(reserve_dispatch(self.db, cfg), [])
        with self.assertRaises(ValueError):
            finish_dispatch(self.db, batch["job_ids"], "wrong-thread", True)
        self.assertEqual(finish_dispatch(self.db, batch["job_ids"], "desktop-thread", False), 2)
        self.db.execute("UPDATE jobs SET retry_after=0")
        second = reserve_dispatch(self.db, cfg)[0]
        self.assertEqual(second["job_ids"], batch["job_ids"])
        self.assertEqual(finish_dispatch(self.db, second["job_ids"], "desktop-thread", True), 2)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM jobs WHERE state='delegated'").fetchone()[0], 2)

    def test_expired_dispatch_lease_checks_session_before_resending(self):
        self.db.execute("INSERT INTO bindings VALUES (?,?,?,?,?)",
                        (self.repo, "pr", 281, "desktop-thread", self.temp.name))
        self.db.execute("INSERT INTO jobs(delivery_id,repo,kind,number,action,created_at,state,retry_after) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        ("event-1", self.repo, "pr", 281, "review_comments.changed",
                         int(time.time()) - 20, "dispatching", int(time.time()) - 1))
        sessions = Path(self.temp.name) / "sessions"
        sessions.mkdir()
        (sessions / "rollout-test-desktop-thread.jsonl").write_text("\n".join(json.dumps(item) for item in [
            {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1",
                                               "started_at": int(time.time()) - 10}},
            {"type": "response_item", "payload": {"type": "message", "role": "user",
                                                 "content": [{"type": "input_text", "text": "event-1"}],
                                                 "internal_chat_message_metadata_passthrough": {"turn_id": "turn-1"}}},
        ]) + "\n")
        cfg = {"repositories": {self.repo: self.temp.name}, "sessions_root": str(sessions)}
        self.assertEqual(reserve_dispatch(self.db, cfg), [])
        self.assertEqual(self.db.execute("SELECT state FROM jobs").fetchone()[0], "waiting_desktop")

    def test_existing_pr_session_lookup_uses_python(self):
        sessions = Path(self.temp.name) / "sessions"
        sessions.mkdir()
        record = sessions / "rollout-test.jsonl"
        url = "https://github.com/example/project/pull/286"
        record.write_text("\n".join(json.dumps(item) for item in [
            {"type": "session_meta", "payload": {"id": "original-thread", "cwd": self.temp.name}},
            {"type": "response_item", "payload": {"type": "custom_tool_call", "input":
             f'tools.mcp__codex_app__attach_artifact({{artifact_type:"pull_request",url:"{url}"}})'}},
        ]))
        cfg = {"repositories": {self.repo: self.temp.name}, "sessions_root": str(sessions)}
        origin = subprocess.CompletedProcess([], 0, "https://github.com/example/project.git\n", "")
        with patch("controller.subprocess.run", return_value=origin) as command:
            self.assertEqual(discover_pr_thread(self.repo, 286, cfg), ("original-thread", self.temp.name))
            command.assert_called_once()

    def test_active_session_is_observed_and_matching_event_is_not_redispatched(self):
        accept(self.db, {self.repo: self.temp.name}, "delivery-286", "pull_request", self.payload)
        self.db.execute("UPDATE jobs SET number=286,event_at=?", (int(time.time()) - 10,))
        row = self.db.execute("SELECT * FROM jobs").fetchone()
        sessions = Path(self.temp.name) / "sessions"
        sessions.mkdir()
        record = sessions / "rollout-test-original-thread.jsonl"
        started = int(time.time()) - 5
        events = [
            {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1", "started_at": started}},
            {"type": "response_item", "payload": {"type": "message", "role": "assistant",
             "content": [{"type": "output_text", "text": "I am reviewing PR #286 comments."}],
             "internal_chat_message_metadata_passthrough": {"turn_id": "turn-1"}}},
        ]
        record.write_text("\n".join(json.dumps(e) for e in events) + "\n")
        cfg = {"repositories": {self.repo: self.temp.name}, "sessions_root": str(sessions)}
        self.assertEqual(session_event_state(row, "original-thread", cfg), "processing")
        self.db.execute("INSERT INTO bindings VALUES (?,?,?,?,?)", (self.repo, "pr", 286, "original-thread", self.temp.name))
        with patch("controller.Codex") as codex:
            self.assertEqual(process_one(self.db, dict(cfg, debounce_seconds=0), False)["observing"], "original-thread")
            codex.assert_not_called()
        with record.open("a") as stream:
            stream.write(json.dumps({"type": "event_msg", "payload": {"type": "task_complete",
                                     "turn_id": "turn-1", "completed_at": int(time.time()),
                                     "last_agent_message": f"Handled Event-ID: {row['delivery_id']}"}}) + "\n")
        self.assertEqual(session_event_state(row, "original-thread", cfg), "handled")
        self.db.execute("UPDATE jobs SET retry_after=0")
        with patch("controller.Codex") as codex:
            self.assertEqual(process_one(self.db, dict(cfg, debounce_seconds=0), False)["already_handled_in"],
                             "original-thread")
            codex.assert_not_called()

    def test_failed_turn_with_event_id_is_not_settled(self):
        accept(self.db, {self.repo: self.temp.name}, "failed-event", "pull_request", self.payload)
        row = self.db.execute("SELECT * FROM jobs").fetchone()
        sessions = Path(self.temp.name) / "sessions"
        sessions.mkdir()
        record = sessions / "rollout-test-original-thread.jsonl"
        now = int(time.time())
        record.write_text("\n".join(json.dumps(item) for item in [
            {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-1", "started_at": now}},
            {"type": "response_item", "payload": {"type": "message", "role": "user",
                "content": [{"type": "input_text", "text": "Event-ID: failed-event"}],
                "internal_chat_message_metadata_passthrough": {"turn_id": "turn-1"}}},
            {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-1",
                "completed_at": now + 1, "error": {"message": "model request failed"}}},
        ]) + "\n")
        cfg = {"repositories": {self.repo: self.temp.name}, "sessions_root": str(sessions)}
        self.assertIsNone(session_event_state(row, "original-thread", cfg))

    def test_review_comments_read_all_pages(self):
        pages = [[{"id": index} for index in range(100)], [{"id": 100}]]
        with patch("controller.subprocess.run", return_value=subprocess.CompletedProcess(
                [], 0, json.dumps(pages), "")) as command:
            self.assertEqual(len(gh_list_pages(f"repos/{self.repo}/pulls/281/comments")), 101)
            self.assertIn("--paginate", command.call_args.args[0])
            self.assertIn("--slurp", command.call_args.args[0])

    def test_continuation_shard_and_long_user_batch_are_read(self):
        accept(self.db, {self.repo: self.temp.name}, "late-event", "pull_request", self.payload)
        row = self.db.execute("SELECT * FROM jobs").fetchone()
        sessions = Path(self.temp.name) / "sessions"
        sessions.mkdir()
        (sessions / "rollout-test-original-thread.jsonl").write_text("")
        now = int(time.time())
        events = [
            {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-2", "started_at": now}},
            {"type": "response_item", "payload": {"type": "message", "role": "user",
                "content": [{"type": "input_text", "text": "x" * 3000 + " Event-ID: late-event"}],
                "internal_chat_message_metadata_passthrough": {"turn_id": "turn-2"}}},
            {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-2",
                "completed_at": now + 1, "last_agent_message": "Handled Event-ID: late-event"}},
        ]
        (sessions / "rollout-test-original-thread_context.jsonl").write_text(
            "\n".join(json.dumps(item) for item in events) + "\n")
        cfg = {"repositories": {self.repo: self.temp.name}, "sessions_root": str(sessions)}
        self.assertEqual(session_event_state(row, "original-thread", cfg), "handled")

    def test_source_specific_review_requires_exact_evidence(self):
        accept(self.db, {self.repo: self.temp.name}, "review-event", "pull_request", self.payload)
        self.db.execute("UPDATE jobs SET action='review_comments.changed',source_id='987',event_at=?",
                        (int(time.time()) - 5,))
        row = self.db.execute("SELECT * FROM jobs").fetchone()
        sessions = Path(self.temp.name) / "sessions"
        sessions.mkdir()
        now = int(time.time())
        record = sessions / "rollout-test-original-thread.jsonl"
        record.write_text("\n".join(json.dumps(item) for item in [
            {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "turn-3", "started_at": now}},
            {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "turn-3",
                "completed_at": now + 1,
                "last_agent_message": f"Reviewed https://github.com/{self.repo}/pull/281 review thread"}},
        ]) + "\n")
        cfg = {"repositories": {self.repo: self.temp.name}, "sessions_root": str(sessions)}
        self.assertIsNone(session_event_state(row, "original-thread", cfg))

    def test_interrupted_running_job_requires_inspection(self):
        accept(self.db, {self.repo: self.temp.name}, "interrupt", "pull_request", self.payload)
        self.db.execute("UPDATE jobs SET state='running'")
        self.assertEqual(recover_interrupted_jobs(self.db), 1)
        self.assertEqual(self.db.execute("SELECT state FROM jobs").fetchone()[0], "needs_inspection")


if __name__ == "__main__":
    unittest.main()
