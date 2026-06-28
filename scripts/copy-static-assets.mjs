import { mkdir, readdir, copyFile } from "node:fs/promises";
import path from "node:path";

async function copySvgFiles(sourceDir) {
  const entries = await readdir(sourceDir, { withFileTypes: true });

  await Promise.all(
    entries.map(async (entry) => {
      const sourcePath = path.join(sourceDir, entry.name);

      if (entry.isDirectory()) {
        await copySvgFiles(sourcePath);
        return;
      }

      if (!entry.isFile() || path.extname(entry.name) !== ".svg") {
        return;
      }

      const relativePath = path.relative("nodes", sourcePath);
      const destinationPath = path.join("dist", "nodes", relativePath);
      await mkdir(path.dirname(destinationPath), { recursive: true });
      await copyFile(sourcePath, destinationPath);
    }),
  );
}

await copySvgFiles("nodes");
