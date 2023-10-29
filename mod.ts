import { Client } from "https://deno.land/x/postgres@v0.17.0/mod.ts";
import { parse as parseFlags } from "https://deno.land/std@0.204.0/flags/mod.ts";
import { parse, Statement } from 'https://deno.land/x/pgsql_ast_parser@11.0.1/mod.ts';
import { walk } from "https://deno.land/std@0.116.0/fs/mod.ts";
import * as filepath from "https://deno.land/std@0.204.0/path/mod.ts";
import { SQLOptions, SQLFile, OrganizerOptions, Options, JSONFile } from "./types.ts";

/** "definitions" are parsed "key value" sql comments comments */
export const getDefinitions = (content: string): {[key: string]: string[], import: string[]} => {
  const m = content.split('\n')
    .filter(line => line.startsWith('--'))
    .map(line => line.replace(/-+/, '').trim())
    .filter(line => line.match(/^(\w+)\s/))
    .map(line => {
      const i = line.split(/^(\w+)\s/)
      return {key: i[1], value: i[2]}
    })
    .reduce((acc, cur) => {
      if (acc[cur.key]) {
        acc[cur.key].push(cur.value)
      } else {
        acc[cur.key] = [cur.value]
      }
      return acc
    }, {} as {[key:string]: string[]})
  return {
    ...m,
    import: [...(m['import'] || [])].flatMap(i => i.split(',').map(v => v.trim())),
  }
}

const AST_TYPE_FUNCTION = 'function'

export const getSQLFile = (path: string, content: string, opts: SQLOptions = {}) => {
  const ast: Statement[] = parse(content);
  const definitions = getDefinitions(content)
  const ctx = getPathContext(path)
  const astType =  ast[0].type === 'create function' ? AST_TYPE_FUNCTION : undefined
  const astName = ast[0].type === 'select' && ast[0].columns && ast[0].columns[0].expr && ast[0].columns[0].expr.type === 'call' && ast[0].columns[0].expr.function && ast[0].columns[0].expr.function.name
  if (opts.assertUsage && astName && astName !== ctx.identifier) {
    throw new Error(`the test ${ctx.base} is named after function ${ctx.identifier} does not not call that function calls ${astName}`)
  }
  if (opts.limitStatement && ast.length > 1) {
    throw new Error(`the file ${ctx.base} has more than one statement`)
  }
  return {
    path,
    content,
    ast,
    definitions,
    ctx,
    astType,
    astName
  }
}

export const assertNoDuplicates = <T>(arr: T[]): void => {
  const counts: Record<string, number> = {};
  const duplicates: T[] = [];
  for (const item of arr) {
    const key = JSON.stringify(item);
    counts[key] = (counts[key] || 0) + 1;
  }
  for (const [key, count] of Object.entries(counts)) {
    if (count > 1) duplicates.push(JSON.parse(key) as T);
  }
  if (duplicates.length > 0) {
    throw new Error(`Project contains duplicate basenames: ${duplicates.join(', ')}`);
  }
};

export const readSqlFile = async (path: string, opts?: SQLOptions): Promise<SQLFile> => {
  const file = await Deno.readTextFile(path)
  return getSQLFile(path, file, opts)
}

export const sortByProp = <T> (arr: T[], handler: (item: T) => string) => {
  return arr.sort((a, b) => {
    const valA = handler(a);
    const valB = handler(b);
    return (valA > valB) ? 1 : ((valB > valA) ? -1 : 0);
  });
};

export const readSQLFiles = async (paths: string[], opts?: SQLOptions) => {
  const files = await Promise.all(paths.map(v => readSqlFile(v, opts)));
  const basenames = files.map(file => file.ctx.base)
  assertNoDuplicates(basenames)
  return sortByProp(files, (file) => file.path)
}

const SUB_IDENTIFIER_TEST = 'test'
const SUB_IDENTIFIER_FAIL = 'fail'
const SUB_IDENTIFIER_OK = 'ok'
const SUB_IDENTIFIERS = [SUB_IDENTIFIER_TEST, SUB_IDENTIFIER_FAIL, SUB_IDENTIFIER_OK]

export const getSubIdentifierType = (value: string | undefined) => {
  if (!value) return undefined
  const type = SUB_IDENTIFIERS.find(t => value && value.startsWith(t)) || undefined
  return type
}

export const isLifecycleValid = (value: string | undefined) => {
  return Boolean(value && GLOBAL_LIFECYCLE_TYPES.includes(value))
}

export function getPathContext (path: string) {
  const parsedPath = filepath.parse(path)
  const snapshot = filepath.join(parsedPath.dir, `${parsedPath.name}.json`)
  const parts = parsedPath.name.split('.')

  let identifier: string | undefined;
  let subIdentifier: string | undefined;
  let lifecycle: string | undefined;
  let subIdentifierOrLifecycle: string | undefined
  if (parts.length === 3) [identifier, subIdentifier, lifecycle] = parts;
  if (parts.length === 2) [identifier, subIdentifierOrLifecycle] = parts;
  if (parts.length === 1) [identifier] = parts;

  if (identifier && GLOBAL_LIFECYCLE_TYPES.includes(identifier)) {
    lifecycle = identifier
  }

  const possibleType = getSubIdentifierType(subIdentifierOrLifecycle)
  if (possibleType) subIdentifier = subIdentifierOrLifecycle
  const subIdentifierType = getSubIdentifierType(subIdentifier)

  const possibleLifecycle = isLifecycleValid(subIdentifierOrLifecycle)
  if (possibleLifecycle) lifecycle = subIdentifierOrLifecycle
  const isValidLifecycleType = isLifecycleValid(lifecycle)
  
  if (lifecycle && !isValidLifecycleType) {
    throw new Error(`invalid lifecycle type: ${lifecycle}`)
  }
  
  if (subIdentifierOrLifecycle && !possibleType && !possibleLifecycle) {
    throw new Error(`invalid definition or lifecycle type: ${subIdentifierOrLifecycle}`)
  }

  if (!identifier) throw new Error(`identifier is undefined`)

  return {
    path,
    ...parsedPath,
    snapshot,
    identifier,
    subIdentifier,
    subIdentifierType,
    lifecycle
  }
}

export async function getFilePaths(dirPath: string) {
  const sql: string[] = [];
  const json: string[] = [];
  for await (const entry of walk(dirPath)) {
    if (entry.isFile && (entry.name.endsWith('.sql'))) {
      sql.push(entry.path);
    }
    if (entry.isFile && (entry.name.endsWith('.json'))) {
      json.push(entry.path);
    }
  }
  return {sql, json};
}

export function sortByDependencies<T>(
  arr: T[],
  nameHandler: (item: T) => string,
  dependsOnHandler: (item: T) => string[]
): T[] {
  const result: T[] = [];
  const visited: { [key: string]: boolean } = {};
  const temp: { [key: string]: boolean } = {};
  function visit(node: T): void {
    const name = nameHandler(node);
    const dependsOn = dependsOnHandler(node);
    if (temp[name]) return;
    if (visited[name]) return;
    temp[name] = true;
    for (const dep of dependsOn) {
      const find = arr.find(n => nameHandler(n) === dep);
      if (!find) throw new Error(`Dependency "${dep}" not found for "${name}"`);
      visit(find);
    }
    visited[name] = true;
    delete temp[name];
    result.push(node);
  }
  for (const item of arr) {
    visit(item);
  }
  return result;
}

export function shakeTreeByTarget<T>(
  arr: T[],
  target: string,
  nameHandler: (item: T) => string,
  dependsOnHandler: (item: T) => string[]
): T[] {
  const result: T[] = [];
  const visited: { [key: string]: boolean } = {};
  function visit(node: T): void {
    const name = nameHandler(node);
    const dependsOn = dependsOnHandler(node);
    if (visited[name]) return;
    visited[name] = true;
    for (const dep of dependsOn) {
      const find = arr.find(n => nameHandler(n) === dep);
      if (!find) throw new Error(`Dependency "${dep}" not found in "${name}"`);
      visit(find);
    }
    result.push(node);
  }
  const targetNode = arr.find(n => nameHandler(n) === target);
  if (!targetNode) throw new Error(`Target "${target}" not found`);
  visit(targetNode);
  return result;
}

// fix to support bundler
const matchTargetNode = (file: SQLFile, action: string, target?: string) => {
  const test = action === TEST_ACTION
  const bundle = action === BUNDLE_ACTION
  if (!test && file.ctx.base === `${target}.sql`) return true
  if (target && file.path.endsWith(target)) return true
  const isRunnable = file.ctx.lifecycle === undefined && file.ctx.subIdentifier !== undefined
  if (bundle && !target && !isRunnable) return true   
  if (test && isRunnable && file.ctx.identifier === target) return true
  if (test && isRunnable && !target) return true
  return false
}

const getShakenIfExists = (files: SQLFile[], key: string) => {
  return files.find(file => file.ctx.name === key)
    ? shakeTreeByTarget(files, key, file => file.ctx.name, file => file.definitions.import)
    : undefined
}

function groupByHandler<T>(array: T[], handler: (item: T) => string): Record<string | number, T[]> {
  return array.reduce((acc, obj) => {
    const key = handler(obj).toString();
    (acc[key] = acc[key] || []).push(obj);
    return acc;
  }, {} as Record<string | number, T[]>);
}

function filterUndefineds<T>(arr: (T | undefined)[]): T[] {
  return arr.filter(item => typeof item !== 'undefined') as T[];
}

const GLOBAL_BEFORE = 'before'
const GLOBAL_BEFORE_EACH_TEST = 'before_each_test'
const GLOBAL_BEFORE_ALL_TEST = 'before_all_test'
const GLOBAL_AFTER = 'after'
const GLOBAL_AFTER_EACH_TEST = 'after_each_test'
const GLOBAL_AFTER_ALL_TEST = 'after_all_test'

const GLOBAL_BEFORE_TYPES = [GLOBAL_BEFORE, GLOBAL_BEFORE_EACH_TEST, GLOBAL_BEFORE_ALL_TEST]
const GLOBAL_AFTER_TYPES = [GLOBAL_AFTER, GLOBAL_AFTER_EACH_TEST, GLOBAL_AFTER_ALL_TEST]
const GLOBAL_LIFECYCLE_TYPES = [...GLOBAL_BEFORE_TYPES, ...GLOBAL_AFTER_TYPES]

async function organizer (sqlPaths: string[], opts: SQLOptions & OrganizerOptions) {
  if (!sqlPaths.length) throw new Error('no sql paths')
  const files = await readSQLFiles(sqlPaths, opts)
  if (!files.length) throw new Error('no files read')
  
  const target = opts?.target
  const targets = files.filter(file => matchTargetNode(file, opts.action, target))
  
  const groupedTargets = groupByHandler(targets, file => file.ctx.identifier)
  const beforeEach = getShakenIfExists(files, GLOBAL_BEFORE_EACH_TEST)
  const afterEach = getShakenIfExists(files, GLOBAL_AFTER_EACH_TEST)

  return filterUndefineds([
    getShakenIfExists(files, GLOBAL_BEFORE),
    ...filterUndefineds(Object.entries(groupedTargets).flatMap(([identifier, targets]) => {
      const beforeAllIdentifier = getShakenIfExists(files, `${identifier}.${GLOBAL_BEFORE_ALL_TEST}`)
      const beforeEachIdentifier = getShakenIfExists(files, `${identifier}.${GLOBAL_BEFORE_EACH_TEST}`)
      const afterAllIdentifier = getShakenIfExists(files, `${identifier}.${GLOBAL_AFTER_ALL_TEST}`)
      const afterEachIdentifier = getShakenIfExists(files, `${identifier}.${GLOBAL_AFTER_EACH_TEST}`)

      return filterUndefineds([
        beforeAllIdentifier,
        ...filterUndefineds(targets.flatMap(target => {
          const directTarget = getShakenIfExists(files, target.ctx.name)
          const beforeTarget = getShakenIfExists(files, `${target.ctx.name}.${GLOBAL_BEFORE}`)
          const afterTarget = getShakenIfExists(files, `${target.ctx.name}.${GLOBAL_AFTER}`)
          return [
            beforeEach,
            beforeEachIdentifier,
            beforeTarget,
            directTarget,
            afterTarget,
            afterEachIdentifier,
            afterEach
          ]
        })),
        afterAllIdentifier
      ])
    })),
    getShakenIfExists(files, GLOBAL_AFTER)
  ])
}

export const BUNDLE_ACTION = 'bundle'
export const RESET_ACTION = 'reset'
export const TEST_ACTION = 'test'
export const RUN_ACTION = 'run'

export function args () {
  const args = Deno.args
  const {
    force,
    database: flagDatabase,
    cwd: flagCwd,
    action: flagAction,
    target: flagTarget,
    _ } = parseFlags(args, { boolean: ['force'], string: ['cwd', 'database', 'action', 'target']});
  const database = flagDatabase || Deno.env.get("DATABASE");
  const cwd = flagCwd || Deno.env.get("CWD") || Deno.cwd();  
  const action = flagAction || typeof _[0] === 'string' && _[0] || undefined
  const target = flagTarget || typeof _[1] === 'string' && _[1] || undefined
  if (!action || ![BUNDLE_ACTION, RESET_ACTION, TEST_ACTION, RUN_ACTION].includes(action)) {
    throw new Error(`invalid action ${action}`)
  }
  return {
    database,
    cwd,
    action,
    target,
    force
  }
}

export async function readJSONFile (path: string) {
  const content = await Deno.readTextFile(path)
  const ctx = getPathContext(path)
  return {
    ctx,
    content
  }
}

export function readJSONFiles (paths: string[]) {
  return Promise.all(paths.map(readJSONFile))
}

function removeDuplicatesByHandler<T>(
  arr: T[],
  handler: (item: T) => any,
  shouldExclude: (item: T) => boolean
): T[] {
  const seen = new Set();
  return arr.filter(item => {
    if (shouldExclude(item)) {
      return true; // Exclude this item from duplicate checking
    }
    const key = handler(item);
    if (seen.has(key)) {
      return false;
    }
    seen.add(key);
    return true;
  })
}

export const removeFunction = (functionName: string) => `
DO $$ 
DECLARE 
  func_record RECORD; 
  func_name text := '${functionName}';
BEGIN 
  FOR func_record IN (
    SELECT oid::regprocedure
    FROM pg_proc
    WHERE proname = func_name
  )
  LOOP 
    EXECUTE 'DROP FUNCTION IF EXISTS ' || func_record.oid; 
  END LOOP; 
END $$;
`

async function exeggutor (client: Client, files: SQLFile[], json: JSONFile[], options: Options) {
  const { force } = options
  await Promise.all(files.map(async file => {
    const snapshot = json.find(j => (j.ctx.base === file.ctx.base))
    const snapshotName = `${file.ctx.base}.json`
    try {
      const result = await client.queryArray(file.content)
      if (file.ctx.subIdentifierType === SUB_IDENTIFIER_TEST) {
        if (snapshot && (snapshot.content === JSON.stringify(result))) {
          console.log(`- executing test: ${file.path} ✅ snapshot match`)
        } else if (snapshot && (snapshot.content !== JSON.stringify(result))) {
          console.log(`- executing test: ${file.path} ❌ invalid snapshot match`)
        } else if (!snapshot) {
          await Deno.writeTextFile(snapshotName, JSON.stringify(result))
          console.log(`- executing test: ${file.path} 👀 created snapshot`)
        }
      } else if (file.ctx.subIdentifierType === SUB_IDENTIFIER_FAIL) {
        console.log(`- executing fail: ${file.path} ❌`)
        console.log(`  - should have failed: (${file.definitions.should})`)
      } else {
        console.log(`- executing query: ${file.path} ✅`)
      }
    } catch (e) {
      if (file.ctx.subIdentifierType === SUB_IDENTIFIER_FAIL) {
        console.log(`- executing fail: ${file.path} ✅`)
        console.log(`  - intentionally failed: (${file.definitions.should})`)
        return;
      } else {
        console.log(`- executing query: ${file.path} ❌`)
      }
      const needsRemoved = e.message === 'cannot change return type of existing function'
      if (file.astType === AST_TYPE_FUNCTION && force && needsRemoved) {
        await client.queryArray(removeFunction(file.ctx.identifier))
        console.log(`- removed function: ${file.path} ✅`)
        try {
          await client.queryArray(file.content)
          console.log(`- executing query (attempt 2): ${file.path} ✅`)
        } catch {
          console.log(`- executing query (attempt 2): ${file.path} ❌`)
        }
      }
    }
  }))
}

export async function main (options: Options) {
  const { cwd, action, database } = options

  const paths = await getFilePaths(cwd)
  const { sql, json } = paths;
  
  const jsonFiles = await readJSONFiles(json)

  const trees = await organizer(sql, {...options, action})

  const client = new Client(database) // will default to the same as psql
  await client.connect();

  const flatFiles = trees.flat()

  const rd = removeDuplicatesByHandler(flatFiles, (file) => file.ctx.base, (file) => Boolean(file.ctx.lifecycle))

  if (action === BUNDLE_ACTION) {
    const bundleOutput = rd
      .map(file => file.content).join('\n\n')
    console.log(bundleOutput)
    return
  }

  await exeggutor(client, rd, jsonFiles, options)
  
  await client.end();
}