import subprocess


class GitRepo:
    def __init__(self, repo_path):
        self.repo_path = repo_path

    def _run(self, *args):
        subprocess.run(
            ["git", *args],
            cwd=self.repo_path,
            capture_output=True,
            text=True,
            check=True,
        )

    def init(self):
        self._run("init")
        self._run("commit", "--message", "init", "--allow-empty")

    def commit_all(self, msg=None):
        self._run("add", "--all")
        self._run("commit", "--allow-empty", "--message", msg)
