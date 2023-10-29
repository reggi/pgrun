import { args, main } from "./mod.ts";

try {
  await main(args())
} catch (e) {
  throw e
}
