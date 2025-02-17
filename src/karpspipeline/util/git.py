import subprocess


class GitRepo:
    def __init__(self, repo_path):
        self.repo_path = repo_path

    def _run(self, args):
        try:
            subprocess.run(
                ["git", *args],
                cwd=self.repo_path,
                capture_output=True,
                text=True,
                check=True,
            )
        except subprocess.CalledProcessError as e:
            print(f"Error: {e.stderr.strip()}")

    def init(self):
        self._run("init")
        self._run("commit", "-m", "init", "--allow-empty")

    def commit_all(self):
        self._run("add", "-A")
        self._run("commit", "-m", "updated by pipeline")
