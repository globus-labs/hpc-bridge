// Spawn a command directly, with no shell, as a stdio MCP client starts a server: can Windows execute it at all?
// Usage: node spawn_probe.js <command> [args...]
const { spawn } = require("child_process");
const [cmd, ...args] = process.argv.slice(2);
const child = spawn(cmd, args, { stdio: ["ignore", "inherit", "inherit"] });
child.on("error", (e) => { console.log(`spawn error: ${e.code} (${e.message})`); process.exit(2); });
child.on("exit", (code) => { console.log(`exited with ${code}`); process.exit(code === 0 ? 0 : 1); });
