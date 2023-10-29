import { assertEquals } from "https://deno.land/std@0.204.0/assert/mod.ts";
import { getPathContext, getDefinitions } from "./mod.ts";

Deno.test('getDefinitions', () => {
  assertEquals(getDefinitions(`-- cat meow`), {'cat': ['meow'], 'import': []})
  assertEquals(getDefinitions(`-- cat meow\n-- cat meow2`), {'cat': ['meow', 'meow2'], 'import': []})
  assertEquals(getDefinitions(`-- import meow,meow2`), {'import': ['meow', 'meow2']})
})

Deno.test('getPathContext - path context with no definition or lifecycle', () => {
  assertEquals(getPathContext('/user/thomas/my-function.sql'), {
      path: "/user/thomas/my-function.sql",
      root: "/",
      dir: "/user/thomas",
      base: "my-function.sql",
      ext: ".sql",
      name: "my-function",
      snapshot: "/user/thomas/my-function.json",
      lifecycle: undefined,
      subIdentifier: undefined,
      subIdentifierType: undefined,
      identifier: "my-function"
    })
})

Deno.test('getPathContext - path context with definition no lifecycle', () => {
  assertEquals(getPathContext('/user/thomas/my-function.test.sql'), {
      path: "/user/thomas/my-function.test.sql",
      root: "/",
      dir: "/user/thomas",
      base: "my-function.test.sql",
      ext: ".sql",
      name: "my-function.test",
      snapshot: "/user/thomas/my-function.test.json",
      lifecycle: undefined,
      subIdentifier: "test",
      subIdentifierType: "test",
      identifier: "my-function"
    })
})

Deno.test('getPathContext - path context with definition and lifecycle', () => {
  assertEquals(getPathContext('/user/thomas/my-function.test.after.sql'), {
      path: "/user/thomas/my-function.test.after.sql",
      root: "/",
      dir: "/user/thomas",
      base: "my-function.test.after.sql",
      ext: ".sql",
      name: "my-function.test.after",
      snapshot: "/user/thomas/my-function.test.after.json",
      lifecycle: 'after',
      subIdentifier: "test",
      subIdentifierType: "test",
      identifier: "my-function"
    })
})