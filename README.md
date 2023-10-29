# pgrun - Postgres Runner

> SQL Bundler and Test runner.

## Installation

```bash
deno install --allow-read --allow-env --allow-net https://deno.land/x/pgrun/bin.ts --name pgrun
```

## Example Scripts (for author)

## `pgrun test`

```bash
pgrun test \
--database postgres://thomasreggi@localhost:28816/json_schema_3 \
--cwd /Users/thomasreggi/Desktop/pg
```

## `pgrun bundle`

```bash
pgrun bundle \
--database postgres://thomasreggi@localhost:28816/json_schema_3 \
--cwd /Users/thomasreggi/Desktop/pg
```