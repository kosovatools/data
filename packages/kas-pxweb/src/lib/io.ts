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

  let shouldWrite = true;
  try {
    const existingContent = await fs.readFile(filePath, "utf8");
    const existing = JSON.parse(existingContent);

    // Normalize both by removing generated_at from meta if present
    const cleanExisting = { ...existing };
    const cleanData = { ...(data as any) };

    if (cleanExisting.meta) {
      cleanExisting.meta = { ...cleanExisting.meta, updated_at: null, generated_at: null };
    }
    if (cleanData.meta) {
      cleanData.meta = { ...cleanData.meta, updated_at: null, generated_at: null };
    }

    // Also need to handle top-level generated_at if it exists (though usually in meta)
    if ("generated_at" in cleanExisting) delete cleanExisting.generated_at;
    if ("generated_at" in cleanData) delete cleanData.generated_at;

    if (jsonStringify(cleanExisting) === jsonStringify(cleanData)) {
      shouldWrite = false;
    }
  } catch (error) {
    // If file doesn't exist or isn't valid JSON, we write.
    shouldWrite = true;
  }

  if (shouldWrite) {
    await fs.writeFile(filePath, jsonStringify(data), "utf8");
    const relative = path.relative(WORKSPACE_ROOT, filePath);
    console.log(`✔ wrote ${relative || filePath}`);
  } else {
    // console.log(`✔ unchanged ${name}`); // Optional: keep output clean or verbose
  }
}
