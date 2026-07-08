# Contributing a data source

All contributions flow through forks and pull requests — nobody pushes directly to this repo. Changes are merged into the **`dev`** branch.

## 1. Fork and clone

Fork [Digital-Health-Agency/EVD-Surveillance-scripts-v1](https://github.com/Digital-Health-Agency/EVD-Surveillance-scripts-v1) on GitHub (or `gh repo fork --clone`), then:

```bash
git clone git@github.com:<your-username>/EVD-Surveillance-scripts-v1.git
cd EVD-Surveillance-scripts-v1
git remote add upstream https://github.com/Digital-Health-Agency/EVD-Surveillance-scripts-v1.git
uv sync
```

## 2. Create a feature branch

Branch off `dev`, named after what you're adding:

```bash
git fetch upstream
git checkout -b features/my-datasource upstream/dev
```

Use `features/<datasource-name>` for new sources (e.g. `features/mdharura`), `fix/<short-description>` for bug fixes.

## 3. Build your data source

Follow the [developer walkthrough](developer-walkthrough.md) (full tutorial) or [adding-a-source.md](adding-a-source.md) (quick reference). Before pushing, make sure:

```bash
dg check defs    # passes
DESTINATION__FILESYSTEM__BUCKET_URL="file:///tmp/dlt-test" \
  dg launch --assets "my_source/<resource>"    # materializes successfully
```

## 4. Commit and push to your fork

```bash
git add src/datasources/defs/my_datasource/
git commit -m "Add my_datasource source"
git push -u origin features/my-datasource
```

Never commit secrets — `.dlt/secrets.toml` is gitignored; keep it that way. API tokens and credentials belong in `dlt.secrets` / env vars ([details](pipelines-and-destinations.md#configuration--secrets)).

## 5. Open a pull request

Open a PR from your fork's `features/my-datasource` branch **into `dev`** on the upstream repo (GitHub defaults to `main` — change the base branch). In the description, cover:

- What the data source is and where the data comes from (API docs link)
- Pagination/incremental strategy and expected data volume
- How you tested it (the [PR checklist](adding-a-source.md#checklist-before-opening-a-pr))

## 6. Keep your fork in sync

```bash
git fetch upstream
git checkout dev && git merge --ff-only upstream/dev
git push origin dev
```

Rebase long-running feature branches on `dev` rather than merging `dev` into them.
