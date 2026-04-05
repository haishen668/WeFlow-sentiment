import { spawn } from 'child_process'
import path from 'path'
import { fileURLToPath } from 'url'
import fs from 'fs'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const rootDir = path.resolve(__dirname, '..')
const electronCacheDir = path.join(rootDir, '.cache', 'electron')

function run(command, args, cwd = rootDir, extraEnv = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: 'inherit',
      shell: true,
      env: {
        ...process.env,
        ...extraEnv
      }
    })
    child.on('exit', (code) => {
      if (code === 0) resolve()
      else reject(new Error(`${command} ${args.join(' ')} failed with exit code ${code}`))
    })
  })
}

async function main() {
  fs.mkdirSync(electronCacheDir, { recursive: true })
  await run('npm', ['--prefix', './web', 'run', 'build'])
  await run('npm', ['--prefix', './server', 'install'])
  await run('npx', ['electron-rebuild', '--module-dir', './server', '--force', '--only', 'better-sqlite3'])

  const rootNodeModulesExists = fs.existsSync(path.join(rootDir, 'node_modules', 'electron'))
  if (!rootNodeModulesExists) {
    await run('npm', ['install'])
  }

  const electronZipName = 'electron-v36.9.5-win32-x64.zip'
  const localElectronZip = path.join(electronCacheDir, electronZipName)
  if (!fs.existsSync(localElectronZip)) {
    console.warn(`[build-release] Electron cache missing: ${localElectronZip}`)
    console.warn('[build-release] If the network is blocked, manually place the Electron zip there before packaging.')
  }
}

main().catch((error) => {
  console.error('[build-release] failed', error)
  process.exit(1)
})
