from collections import defaultdict


def read_file(filename):
    lines = set()
    with open(filename) as f:
        for line in f.readlines():
            line = line.rstrip()
            line = line.strip("'")
            lines.add(line)
    return lines

test_lines = read_file("v10.1.test_changes.txt")
master_lines = read_file("master_changes.txt")
not_backported = [line for line in master_lines if line not in test_lines]
backported = [line for line in master_lines if line in test_lines]

groupable_terms = [
    "test",
    "rename",
    "error",
    "docs",
    "fix",
    "import",
    "library",
    "contrib",
    "dependen",
    "revert",
    "config",
    "api",
    "remove",
    "add",
    "update",
    "google",
    "gcs",
    "gcp",
    "gke",
    "azure",
    "docker",
    "kube",
    "aws",
    "sql",
    "db",
]

skippable_terms = [
    "typo",
    "readme",
    "airflow user",
    "annotat",
    "compan",
    "docs",
    "document",
    "breeze",
    "gitignore",
    "lint",
    "coverage",
    "type",
    "version",
    "changelog",
    "upgrad",
    "quarantine",
    ".md",
    "openapi",
    "table",
    "pre-commit",
    "refactor",
    "spell",
]

countable_terms = skippable_terms + groupable_terms
grouped_terms = defaultdict(list)
counts = defaultdict(int)
for line in not_backported:
    skipped = False
    for term in countable_terms:
        if not skipped and term in line.lower():
            counts[term] += 1
            skipped = True
    if not skipped:
        counts["misc"] += 1
        grouped_terms["misc"].append(line)

    for term in groupable_terms:
        if term in line.lower():
            grouped_terms[term].append(line)

for term, issues in grouped_terms.items():
    print(f"Term: {term}")
    for issue in issues:
        print(f"-{issue}")

total = 0
for key in sorted(counts):
    print(f"{key}: {counts[key]}")
    total += counts[key]

assert total + len(backported) == len(master_lines)