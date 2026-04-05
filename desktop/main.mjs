import { app, BrowserWindow } from 'electron'
import path from 'path'
import { fileURLToPath, pathToFileURL } from 'url'
import { existsSync } from 'fs'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const projectRoot = app.isPackaged ? path.join(process.resourcesPath, 'app.asar.unpacked') : path.resolve(__dirname, '..')
const serverEntry = path.join(projectRoot, 'server', 'index.js')
const webDist = path.join(projectRoot, 'web', 'dist', 'index.html')

let mainWindow = null
let serverModule = null
let serverStartPromise = null

if (!app.requestSingleInstanceLock()) {
  app.quit()
}

function wait(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

async function waitForServer(url, retries = 60) {
  for (let i = 0; i < retries; i += 1) {
    try {
      const response = await fetch(url)
      if (response.ok) return true
    } catch {
      // ignore until ready
    }
    await wait(1000)
  }
  throw new Error(`Server did not become ready: ${url}`)
}

async function ensureServerStarted() {
  if (serverModule) {
    await serverModule.startServer()
    return serverModule
  }
  if (serverStartPromise) {
    return serverStartPromise
  }
  if (!existsSync(serverEntry)) {
    throw new Error(`Server entry not found: ${serverEntry}`)
  }

  serverStartPromise = (async () => {
    const module = await import(pathToFileURL(serverEntry).href)
    if (typeof module.startServer !== 'function') {
      throw new Error('Server module does not export startServer()')
    }
    serverModule = module
    await module.startServer()
    return module
  })()

  try {
    return await serverStartPromise
  } finally {
    serverStartPromise = null
  }
}

async function createWindow() {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.focus()
    return
  }

  await ensureServerStarted()
  await waitForServer('http://127.0.0.1:5403/api/health')

  mainWindow = new BrowserWindow({
    width: 1440,
    height: 960,
    autoHideMenuBar: true,
    webPreferences: {
      preload: path.join(__dirname, 'preload.mjs')
    }
  })

  mainWindow.on('closed', () => {
    mainWindow = null
  })

  await mainWindow.loadFile(webDist)
}

app.on('second-instance', () => {
  if (mainWindow && !mainWindow.isDestroyed()) {
    if (mainWindow.isMinimized()) mainWindow.restore()
    mainWindow.focus()
    return
  }

  createWindow().catch((error) => {
    console.error('[desktop] failed to restore existing instance', error)
  })
})

app.whenReady().then(() => {
  createWindow().catch((error) => {
    console.error('[desktop] failed to start', error)
    app.quit()
  })

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      createWindow().catch((error) => {
        console.error('[desktop] failed to reactivate', error)
      })
    }
  })
})

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') app.quit()
})

app.on('before-quit', () => {
  if (serverModule?.stopServer) {
    serverModule.stopServer().catch((error) => {
      console.error('[desktop] failed to stop server', error)
    })
  }
})
