import subprocess


def run_command(commands: list):
    result = subprocess.run(commands, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=True)
    if result.returncode != 0:
        print(f"Error running command: {result.stderr}")
        exit(1)
    return result.stdout


# HEAD^1 says the "first" parent. For PR merge commits, or main commits, this is the "right" commit.
#
# In this example, 9c532b6 is the PR commit (HEAD^2), 4840892 is the head GitHub checks-out for us, and db121f7 is the
# "merge target" (HEAD^1) -- i.e. mainline
#
# *   4840892 (HEAD, pull/11906/merge) Merge 9c532b6a2c56cd5d4c2a80ecbed60f9dfd1f5fe6 into db121f726b3c7a37aca1ea05eb4714f884456005 [Ephraim Anierobi]
# |\
# | * 9c532b6 (grafted) make taskinstances pid and duration nullable [EphraimBuddy]
# * db121f7 (grafted) Add truncate table (before copy) option to S3ToRedshiftOperator (#9246) [JavierLopezT]


def client_codegen_diff():
    previous_mainline_commit = run_command(["git", "rev-parse", "--short", "HEAD^1"])
    print(f"Diffing openapi spec against {previous_mainline_commit}...")

    SPEC_FILE = "airflow/api_connexion/openapi/v1.yaml"

    GO_CLIENT_PATH = "clients/go/airflow"

    GO_TARGET_CLIENT_PATH = "clients/go_target_branch/airflow"

    run_command(["mkdir", "-p", f"{GO_CLIENT_PATH}"])
    run_command(["./clients/gen/go.sh", f"{SPEC_FILE}", f"{GO_CLIENT_PATH}"])

    # generate client for target patch
    run_command(["mkdir" "-p" f"{GO_TARGET_CLIENT_PATH}"])

    run_command(["git", "checkout", f"{previous_mainline_commit}", "--", "{SPEC_FILE}"])
    run_command(["./clients/gen/go.sh", f"{SPEC_FILE}", f"{GO_TARGET_CLIENT_PATH}"])
    run_command(["diff", "-u", f"{GO_TARGET_CLIENT_PATH}", f"{GO_CLIENT_PATH}", "||", "true"])


if __name__ == '__main__':
    client_codegen_diff()
