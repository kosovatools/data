import fs from "node:fs/promises";
import path from "node:path";

import { jsonStringify } from "./utils";

const WORKSPACE_ROOT =
  process.env.WORKSPACE_ROOT || process.env.INIT_CWD || process.cwd();

export async function writeJson(
  outDir: string,
  name: string,
  data: unknown,
): Promise<void> {
  await fs.mkdir(outDir, { recursive: true });
  const filePath = path.join(outDir, name);
  await fs.writeFile(filePath, jsonStringify(data), "utf8");
  const relative = path.relative(WORKSPACE_ROOT, filePath);
  console.log(`✔ wrote ${relative || filePath}`);
}
