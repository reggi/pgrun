import type { getSQLFile, readJSONFile } from './mod.ts'

export type SQLFile = ReturnType<typeof getSQLFile>
export type JSONFile = Awaited<ReturnType<typeof readJSONFile>>

export type SQLOptions = {
  assertUsage?: boolean,
  limitStatement?: boolean
}

export type OrganizerOptions = {
  /** the target file(s) to run or bundle */
  target?: string
  /** specific action run (just runs query), bundle (prints bundle), test (validates tests) */
  action: string
}

export type CLIOptions = {
  /** the database URI */
  database: string | undefined
  /** will attempt to remove function when failure and retry */
  force: boolean
  /** the current working directory */
  cwd: string
}

export type Options = SQLOptions & OrganizerOptions & CLIOptions