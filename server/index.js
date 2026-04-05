import express from 'express'
import cors from 'cors'
import fs from 'fs/promises'
import path from 'path'
import {
  initDb,
  insertPrices,
  insertPricesWithSignals,
  listPrices,
  listSignals,
  listPendingPushSignals,
  markSignalsPushStatus,
  insertSignals,
  listPendingAliases,
  confirmPendingAlias,
  rejectPendingAlias,
  listAliases,
  hasImportedMessage,
  listProducts,
  getPriceStats,
  getSignalConfig,
  updateSignalConfig,
  updateProductSignalConfig,
  rebuildSignals,
  upsertAlias,
  deleteAlias,
  listProductLibrary,
  listPriceTrendProducts,
  listFavoriteProducts,
  setProductFavorite,
  reapplyAliasMappings,
  getPriceTrendDetail
} from './src/lib/db.js'
import { parseMessagesToPrices, searchProducts } from './src/lib/parser.js'
import { evaluateSignals } from './src/lib/strategy.js'
import { fetchJson } from './src/lib/httpClient.js'
import { buildDailyReportMessage, buildSignalPushMessage, sendServerChanMessage } from './src/lib/notify.js'

const app = express()
app.use(cors())
app.use(express.json())

const DEFAULT_SOURCE_SETTINGS = {
  savedTalkers: [],
  manualIngestDefaults: {
    start: '',
    end: ''
  },
  scheduledIngest: {
    enabled: false,
    intervalMinutes: 10,
    recentDays: 1,
    lastRunAt: '',
    lastRunStatus: '',
    lastRunSummary: null,
    lastRunKey: '',
    logs: []
  }
}

const DEFAULT_NOTIFICATION_SETTINGS = {
  enabled: false,
  sendKey: '',
  signalPushEnabled: false,
  dailyReportEnabled: false,
  dailyReportTime: '21:30',
  lastDailyReportDate: '',
  logs: []
}

const config = {
  weflowApiBase: 'http://127.0.0.1:5031',
  weflowToken: '',
  lexiconApiBase: 'http://127.0.0.1:8000/lexicon',
  lexiconToken: '',
  llmBaseUrl: process.env.LLM_BASE_URL || 'https://geek.tm2.xin',
  llmApiKey: process.env.LLM_API_KEY || '',
  llmModel: process.env.LLM_MODEL || 'gpt-4o-mini',
  llmBatchSize: 20,
  sourceSettings: structuredClone(DEFAULT_SOURCE_SETTINGS),
  notificationSettings: structuredClone(DEFAULT_NOTIFICATION_SETTINGS)
}

const CONFIG_PATH = path.resolve(process.cwd(), 'data', 'config.json')

function normalizeSourceSettings(input = {}) {
  const scheduled = input?.scheduledIngest || {}
  const manual = input?.manualIngestDefaults || {}
  const savedTalkers = Array.isArray(input?.savedTalkers)
    ? Array.from(new Set(input.savedTalkers.map((item) => String(item || '').trim()).filter(Boolean)))
    : []
  const logs = Array.isArray(scheduled.logs)
    ? scheduled.logs.map((item) => ({
        id: String(item?.id || ''),
        status: item?.status === 'error' ? 'error' : 'success',
        runAt: String(item?.runAt || ''),
        summary: item?.summary && typeof item.summary === 'object' ? item.summary : null,
        error: String(item?.error || '')
      })).filter((item) => item.id && item.runAt)
    : []

  return {
    savedTalkers,
    manualIngestDefaults: {
      start: String(manual.start || ''),
      end: String(manual.end || '')
    },
    scheduledIngest: {
      enabled: Boolean(scheduled.enabled),
      intervalMinutes: [10, 20, 30, 60].includes(Number(scheduled.intervalMinutes)) ? Number(scheduled.intervalMinutes) : 10,
      recentDays: [1, 2, 3].includes(Number(scheduled.recentDays)) ? Number(scheduled.recentDays) : 1,
      lastRunAt: String(scheduled.lastRunAt || ''),
      lastRunStatus: String(scheduled.lastRunStatus || ''),
      lastRunSummary: scheduled.lastRunSummary && typeof scheduled.lastRunSummary === 'object' ? scheduled.lastRunSummary : null,
      lastRunKey: String(scheduled.lastRunKey || ''),
      logs: logs.slice(0, 20)
    }
  }
}

function normalizeNotificationLog(item = {}) {
  return {
    id: String(item?.id || ''),
    type: item?.type === 'daily_report' ? 'daily_report' : 'signal_push',
    status: item?.status === 'error' ? 'error' : 'success',
    runAt: String(item?.runAt || ''),
    title: String(item?.title || ''),
    detail: String(item?.detail || ''),
    error: String(item?.error || '')
  }
}

function normalizeDailyReportTime(value = '') {
  const text = String(value || '').trim()
  return /^([01]\d|2[0-3]):([0-5]\d)$/.test(text) ? text : '21:30'
}

function normalizeNotificationSettings(input = {}) {
  const logs = Array.isArray(input?.logs)
    ? input.logs.map((item) => normalizeNotificationLog(item)).filter((item) => item.id && item.runAt)
    : []

  return {
    enabled: Boolean(input?.enabled),
    sendKey: String(input?.sendKey || '').trim(),
    signalPushEnabled: Boolean(input?.signalPushEnabled),
    dailyReportEnabled: Boolean(input?.dailyReportEnabled),
    dailyReportTime: normalizeDailyReportTime(input?.dailyReportTime),
    lastDailyReportDate: /^\d{4}-\d{2}-\d{2}$/.test(String(input?.lastDailyReportDate || '').trim()) ? String(input.lastDailyReportDate).trim() : '',
    logs: logs.slice(0, 20)
  }
}

function getSourceSettings() {
  return structuredClone(config.sourceSettings)
}

function getNotificationSettings() {
  return structuredClone(config.notificationSettings)
}

function setSourceSettings(nextSettings = {}) {
  config.sourceSettings = normalizeSourceSettings(nextSettings)
  return getSourceSettings()
}

function setNotificationSettings(nextSettings = {}) {
  config.notificationSettings = normalizeNotificationSettings(nextSettings)
  return getNotificationSettings()
}

async function persistSourceSettings() {
  await saveConfigFile()
}

async function persistNotificationSettings() {
  await saveConfigFile()
}

async function loadConfigFile() {
  try {
    const raw = await fs.readFile(CONFIG_PATH, 'utf8')
    const stored = JSON.parse(raw)
    if (typeof stored.llmBaseUrl === 'string') config.llmBaseUrl = stored.llmBaseUrl
    if (typeof stored.llmApiKey === 'string') config.llmApiKey = stored.llmApiKey
    if (typeof stored.llmModel === 'string') config.llmModel = stored.llmModel
    if (Number.isFinite(stored.llmBatchSize)) config.llmBatchSize = stored.llmBatchSize
    config.sourceSettings = normalizeSourceSettings(stored.sourceSettings || stored)
    config.notificationSettings = normalizeNotificationSettings(stored.notificationSettings || {})
  } catch (error) {
    if (error?.code !== 'ENOENT') {
      console.error('[config] load failed', error)
    }
  }
}

async function saveConfigFile() {
  const payload = {
    llmBaseUrl: config.llmBaseUrl,
    llmApiKey: config.llmApiKey,
    llmModel: config.llmModel,
    llmBatchSize: config.llmBatchSize,
    sourceSettings: config.sourceSettings,
    notificationSettings: config.notificationSettings
  }
  await fs.writeFile(CONFIG_PATH, JSON.stringify(payload, null, 2), 'utf8')
}

function hasDigits(text = '') {
  return /\d/.test(text)
}

function buildMemoryPriceDedupeKey(row = {}) {
  return [
    row.talker || '',
    row.sender || '',
    row.senderDisplayName || '',
    row.messageId || row.platformMessageId || '',
    row.type || '',
    Number(row.price) || '',
    Number(row.timestamp) || '',
    String(row.brandRaw || row.brand || '').replace(/\s+/g, '').toLowerCase(),
    String(row.rawText || '').replace(/\s+/g, '').toLowerCase()
  ].join('||')
}

function groupRowsByMessageId(rows = []) {
  const grouped = new Map()
  for (const row of rows) {
    const messageId = String(row?.messageId || row?.platformMessageId || '').trim()
    if (!messageId) continue
    if (!grouped.has(messageId)) grouped.set(messageId, [])
    grouped.get(messageId).push(row)
  }
  return grouped
}

async function filterNewMessages(messages = [], parsedRowsByMessage = new Map()) {
  const fresh = []
  let skipped = 0

  for (const msg of messages) {
    const messageId = String(msg?.platformMessageId || msg?.messageId || '').trim()
    const relatedRows = messageId ? (parsedRowsByMessage.get(messageId) || []) : []
    if (messageId && await hasImportedMessage(messageId, relatedRows)) {
      skipped += 1
      continue
    }
    fresh.push(msg)
  }

  return { fresh, skipped }
}

function buildLlmOptions() {
  return config.llmApiKey
    ? {
        baseUrl: config.llmBaseUrl,
        apiKey: config.llmApiKey,
        model: config.llmModel,
        batchSize: config.llmBatchSize
      }
    : null
}

function createScheduledLog(status, summary = null) {
  const runAt = new Date().toISOString()
  return {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    status: status === 'error' ? 'error' : 'success',
    runAt,
    summary: status === 'success' && summary && typeof summary === 'object' ? summary : null,
    error: status === 'error' ? String(summary?.error || summary || '') : ''
  }
}

function createNotificationLog(type, status, payload = {}) {
  const runAt = new Date().toISOString()
  return normalizeNotificationLog({
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    type,
    status,
    runAt,
    title: payload?.title || '',
    detail: payload?.detail || '',
    error: status === 'error' ? String(payload?.error || '') : ''
  })
}

async function appendNotificationLog(type, status, payload = {}) {
  const settings = getNotificationSettings()
  const log = createNotificationLog(type, status, payload)
  settings.logs = [log, ...(Array.isArray(settings.logs) ? settings.logs : [])].slice(0, 20)
  setNotificationSettings(settings)
  await persistNotificationSettings()
  return log
}

app.get('/api/health', (_req, res) => {
  res.json({ status: 'ok' })
})

app.get('/api/config', (_req, res) => {
  res.json({
    apiBase: `http://127.0.0.1:${listenPort}`,
    weflowApiBase: config.weflowApiBase,
    lexiconApiBase: config.lexiconApiBase
  })
})

app.get('/api/sessions', async (req, res) => {
  const { keyword, limit } = req.query || {}
  try {
    const url = new URL('/api/v1/sessions', config.weflowApiBase)
    if (keyword) url.searchParams.set('keyword', String(keyword))
    if (limit) url.searchParams.set('limit', String(limit))

    const headers = {}
    if (config.weflowToken) headers.Authorization = `Bearer ${config.weflowToken}`

    const payload = await fetchJson(url.toString(), { headers })
    const sessions = (payload?.sessions || []).filter((item) => item?.username?.endsWith('@chatroom'))
    res.json({ ...payload, sessions, count: sessions.length })
  } catch (error) {
    res.status(500).json({ error: String(error) })
  }
})

app.post('/api/llm/config', async (req, res) => {
  const { baseUrl, apiKey, model, batchSize } = req.body || {}
  if (baseUrl) config.llmBaseUrl = String(baseUrl)
  if (apiKey) config.llmApiKey = String(apiKey)
  if (model) config.llmModel = String(model)
  if (batchSize) config.llmBatchSize = Number(batchSize)
  try {
    await saveConfigFile()
  } catch (error) {
    res.status(500).json({ error: String(error) })
    return
  }
  res.json({
    llmBaseUrl: config.llmBaseUrl,
    llmModel: config.llmModel,
    llmBatchSize: config.llmBatchSize,
    llmApiKey: config.llmApiKey ? '***' : ''
  })
})

app.get('/api/llm/config', (_req, res) => {
  res.json({
    llmBaseUrl: config.llmBaseUrl,
    llmModel: config.llmModel,
    llmBatchSize: config.llmBatchSize,
    llmApiKey: config.llmApiKey ? '***' : ''
  })
})

async function readLocalTextFile(filename) {
  const safeName = path.basename(filename || '')
  const filePath = path.resolve(process.cwd(), '../texts', safeName)
  const raw = await fs.readFile(filePath, 'utf8')
  return JSON.parse(raw)
}

function sanitizeParsedPrices(rows = []) {
  const accepted = []
  const blocked = []

  for (const row of rows) {
    const normalizedSource = String(row?.normalizationSource || '').trim()
    const confidence = Number(row?.confidence) || 0
    const productUuid = String(row?.productUuid || '').trim()
    const type = String(row?.type || '').trim()
    const price = Number(row?.price)
    const timestamp = Number(row?.timestamp)

    if (!Number.isFinite(price) || price <= 0) {
      blocked.push(row)
      continue
    }
    if (!Number.isFinite(timestamp) || timestamp <= 0) {
      blocked.push(row)
      continue
    }
    if (confidence < 0.6 || !productUuid || normalizedSource === 'raw' || type === 'unknown') {
      blocked.push(row)
      continue
    }

    accepted.push(row)
  }

  return { accepted, blocked }
}

async function persistParsedPrices(priceRows = []) {
  if (!priceRows.length) return { insertedPrices: 0, insertedSignals: 0 }
  const signalConfig = await getSignalConfig()
  const signals = evaluateSignals(priceRows, signalConfig)
  await insertPricesWithSignals(priceRows, signals)
  return { insertedPrices: priceRows.length, insertedSignals: signals.length }
}

async function runIngestJob({ talkers = [], start, end, limit, offset, keyword } = {}) {
  const targetTalkers = Array.isArray(talkers)
    ? talkers.filter(Boolean)
    : []
  if (targetTalkers.length === 0) {
    throw new Error('Missing talker')
  }

  const headers = {}
  if (config.weflowToken) headers.Authorization = `Bearer ${config.weflowToken}`

  const allMessages = []
  for (const target of targetTalkers) {
    const url = new URL('/api/v1/messages', config.weflowApiBase)
    url.searchParams.set('talker', target)
    if (start) url.searchParams.set('start', String(start))
    if (end) url.searchParams.set('end', String(end))
    if (keyword) url.searchParams.set('keyword', String(keyword))
    if (limit) url.searchParams.set('limit', String(limit))
    else url.searchParams.set('limit', '10000')
    if (offset) url.searchParams.set('offset', String(offset))

    const payload = await fetchJson(url.toString(), { headers })
    if (payload?.messages?.length) {
      payload.messages.forEach((msg) => {
        msg.talker = target
      })
      allMessages.push(...payload.messages)
    }
  }

  if (!allMessages.length) {
    throw new Error('Upstream returned no messages')
  }

  const parsed = await parseMessagesToPrices(allMessages, {
    lexiconApiBase: config.lexiconApiBase,
    lexiconToken: config.lexiconToken,
    llm: buildLlmOptions()
  })
  const parserRowsByMessage = groupRowsByMessageId(parsed.prices || [])
  const { fresh: newMessages, skipped } = await filterNewMessages(allMessages, parserRowsByMessage)

  if (!newMessages.length) {
    return { ingested: 0, candidates: 0, prices: 0, signals: 0, lowConfidence: 0, blocked: 0, llmParsed: 0, skipped }
  }

  const freshMessageIds = new Set(newMessages.map((msg) => String(msg?.platformMessageId || msg?.messageId || '').trim()).filter(Boolean))
  const freshParsedPrices = (parsed.prices || []).filter((row) => {
    const messageId = String(row?.messageId || '').trim()
    return !messageId || freshMessageIds.has(messageId)
  })

  if (!freshParsedPrices.length) {
    return { ingested: newMessages.length, candidates: 0, prices: 0, signals: 0, lowConfidence: 0, blocked: 0, llmParsed: 0, skipped }
  }

  const uniquePrices = Array.from(
    new Map(freshParsedPrices.map((row) => [buildMemoryPriceDedupeKey(row), row])).values()
  )
  const { accepted, blocked } = sanitizeParsedPrices(uniquePrices)
  const persisted = await persistParsedPrices(accepted)

  return {
    ingested: newMessages.length,
    candidates: freshParsedPrices.length,
    prices: persisted.insertedPrices,
    signals: persisted.insertedSignals,
    lowConfidence: parsed.lowConfidence?.length || 0,
    blocked: blocked.length,
    llmParsed: 0,
    skipped
  }
}

function getScheduleRunKey(settings, now = new Date()) {
  const scheduled = settings?.scheduledIngest || {}
  const intervalMinutes = Math.max(1, Number(scheduled.intervalMinutes) || 10)
  const bucket = Math.floor(now.getTime() / (intervalMinutes * 60 * 1000))
  return `interval:${intervalMinutes}:${bucket}`
}

function resolveRecentDateRange(recentDays = 1) {
  const safeDays = [1, 2, 3].includes(Number(recentDays)) ? Number(recentDays) : 1
  const end = new Date()
  const start = new Date()
  start.setDate(start.getDate() - safeDays)
  return {
    start: `${start.getFullYear()}${`${start.getMonth() + 1}`.padStart(2, '0')}${`${start.getDate()}`.padStart(2, '0')}`,
    end: `${end.getFullYear()}${`${end.getMonth() + 1}`.padStart(2, '0')}${`${end.getDate()}`.padStart(2, '0')}`
  }
}

function getTodayIsoDate(now = new Date()) {
  return `${now.getFullYear()}-${`${now.getMonth() + 1}`.padStart(2, '0')}-${`${now.getDate()}`.padStart(2, '0')}`
}

function hasReachedDailyReportTime(timeText = '', now = new Date()) {
  const [hourText, minuteText] = String(timeText || '').split(':')
  const hour = Number(hourText)
  const minute = Number(minuteText)
  if (!Number.isInteger(hour) || !Number.isInteger(minute)) return false
  const currentMinutes = now.getHours() * 60 + now.getMinutes()
  const targetMinutes = hour * 60 + minute
  return currentMinutes >= targetMinutes
}

function summarizeTodayScheduledRuns(logs = [], today = getTodayIsoDate()) {
  const rows = Array.isArray(logs) ? logs.filter((item) => String(item?.runAt || '').slice(0, 10) === today) : []
  return rows.reduce((acc, log) => {
    acc.totalRuns += 1
    if (log.status === 'error') {
      acc.errorRuns += 1
      return acc
    }
    acc.successRuns += 1
    const summary = log.summary || {}
    acc.ingested += Number(summary.ingested || 0)
    acc.prices += Number(summary.prices || 0)
    acc.signals += Number(summary.signals || 0)
    acc.blocked += Number(summary.blocked || 0)
    return acc
  }, {
    totalRuns: 0,
    successRuns: 0,
    errorRuns: 0,
    ingested: 0,
    prices: 0,
    signals: 0,
    blocked: 0
  })
}

async function sendSignalPushIfNeeded() {
  const settings = getNotificationSettings()
  if (!settings.enabled || !settings.signalPushEnabled) return
  if (!settings.sendKey) {
    await appendNotificationLog('signal_push', 'error', {
      title: '信号推送未执行',
      error: '缺少 Server酱 SendKey'
    })
    return
  }

  const signals = await listPendingPushSignals(10)
  if (!signals.length) return

  const batchId = `signal-${Date.now()}`
  const signalIds = signals.map((item) => item.id)
  await markSignalsPushStatus(signalIds, {
    status: 'pending',
    batchId,
    error: '',
    pushedAt: null,
    incrementAttempts: true
  })

  const message = buildSignalPushMessage(signals)
  try {
    await sendServerChanMessage(settings.sendKey, {
      ...message,
      tags: '信号提醒'
    })
  } catch (firstError) {
    try {
      await sendServerChanMessage(settings.sendKey, {
        ...message,
        tags: '信号提醒'
      })
    } catch (secondError) {
      await markSignalsPushStatus(signalIds, {
        status: 'failed',
        batchId,
        error: String(secondError),
        pushedAt: null,
        incrementAttempts: false
      })
      await appendNotificationLog('signal_push', 'error', {
        title: message.title,
        error: String(secondError)
      })
      return
    }
  }

  await markSignalsPushStatus(signalIds, {
    status: 'success',
    batchId,
    error: '',
    pushedAt: new Date().toISOString(),
    incrementAttempts: false
  })
  await appendNotificationLog('signal_push', 'success', {
    title: message.title,
    detail: `已推送 ${signals.length} 条信号`
  })
}

async function tickDailyReport() {
  const settings = getNotificationSettings()
  if (!settings.enabled || !settings.dailyReportEnabled) return
  if (!settings.sendKey) return

  const now = new Date()
  const today = getTodayIsoDate(now)
  if (settings.lastDailyReportDate === today) return
  if (!hasReachedDailyReportTime(settings.dailyReportTime, now)) return

  const signalRows = await listSignals()
  const keySignals = signalRows
    .filter((item) => String(item?.timestamp || '').startsWith('') || true)
    .filter((item) => String(item?.timestamp || 0))
    .filter((item) => getTodayIsoDate(new Date(Number(item.timestamp) * 1000)) === today)
    .sort((a, b) => Math.abs(Number(b.lossPct || 0)) - Math.abs(Number(a.lossPct || 0)))
    .slice(0, 5)

  const favorites = await listFavoriteProducts()
  const favoriteActivity = []
  for (const item of favorites.slice(0, 8)) {
    const detail = await getPriceTrendDetail({ productUuid: item.productUuid, activityRange: '1d' })
    const latestRecord = detail?.records?.[0]
    if (!latestRecord) continue
    favoriteActivity.push({
      productUuid: item.productUuid,
      name: item.name,
      latestPrice: latestRecord.price,
      latestAt: latestRecord.timestamp ? new Date(Number(latestRecord.timestamp) * 1000).toLocaleString('zh-CN', { hour12: false }) : '-',
      latestType: latestRecord.type || ''
    })
  }

  const sourceSettings = getSourceSettings()
  const ingestSummary = summarizeTodayScheduledRuns(sourceSettings?.scheduledIngest?.logs || [], today)
  const notificationLogs = (settings.logs || []).filter((item) => String(item?.runAt || '').slice(0, 10) === today && item.status === 'error')
  const message = buildDailyReportMessage({
    reportDate: today,
    keySignals,
    favorites: favoriteActivity,
    ingestSummary,
    errors: notificationLogs.map((item) => item.error || item.title || '通知失败').slice(0, 5)
  })

  try {
    await sendServerChanMessage(settings.sendKey, {
      ...message,
      tags: '每日报告|重点信号'
    })
    setNotificationSettings({
      ...settings,
      lastDailyReportDate: today
    })
    await persistNotificationSettings()
    await appendNotificationLog('daily_report', 'success', {
      title: message.title,
      detail: `重点信号 ${keySignals.length} 条，收藏动态 ${favoriteActivity.length} 条`
    })
  } catch (error) {
    await appendNotificationLog('daily_report', 'error', {
      title: message.title,
      error: String(error)
    })
  }
}

function shouldRunSchedule(settings, now = new Date()) {
  const scheduled = settings?.scheduledIngest || {}
  if (!scheduled.enabled) return false
  if (!Array.isArray(settings?.savedTalkers) || settings.savedTalkers.length === 0) return false

  const runKey = getScheduleRunKey(settings, now)
  return runKey !== String(scheduled.lastRunKey || '')
}

async function updateScheduledRun(status, summary = null) {
  const settings = getSourceSettings()
  const log = createScheduledLog(status, summary)
  settings.scheduledIngest.lastRunAt = log.runAt
  settings.scheduledIngest.lastRunStatus = status
  settings.scheduledIngest.lastRunSummary = summary
  settings.scheduledIngest.lastRunKey = getScheduleRunKey(settings, new Date())
  settings.scheduledIngest.logs = [log, ...(Array.isArray(settings.scheduledIngest.logs) ? settings.scheduledIngest.logs : [])].slice(0, 20)
  setSourceSettings(settings)
  await persistSourceSettings()
}

async function tickScheduledIngest() {
  const settings = getSourceSettings()
  if (!shouldRunSchedule(settings)) return

  try {
    const range = resolveRecentDateRange(settings.scheduledIngest.recentDays)
    const summary = await runIngestJob({
      talkers: settings.savedTalkers,
      start: range.start,
      end: range.end
    })
    await updateScheduledRun('success', {
      ...summary,
      start: range.start,
      end: range.end,
      recentDays: settings.scheduledIngest.recentDays
    })
    await sendSignalPushIfNeeded()
  } catch (error) {
    console.error('[schedule] ingest failed', error)
    await updateScheduledRun('error', { error: String(error) })
  }
}

app.post('/api/ingest/file', async (req, res) => {
  const { filename } = req.body || {}
  if (!filename) {
    res.status(400).json({ error: 'Missing filename' })
    return
  }

  try {
    const payload = await readLocalTextFile(filename)
    const messages = payload?.messages || []
    const talker = payload?.session?.nickname || payload?.session?.displayName || payload?.session?.wxid || filename
    messages.forEach((msg) => {
      msg.talker = talker
    })

    if (!messages.length) {
      res.status(400).json({ error: 'No messages in file' })
      return
    }

    const parsed = await parseMessagesToPrices(messages, {
      lexiconApiBase: config.lexiconApiBase,
      lexiconToken: config.lexiconToken,
      llm: buildLlmOptions()
    })
    const parserRowsByMessage = groupRowsByMessageId(parsed.prices || [])
    const { fresh: newMessages, skipped } = await filterNewMessages(messages, parserRowsByMessage)

    if (!newMessages.length) {
      res.json({ ingested: 0, candidates: 0, prices: 0, signals: 0, lowConfidence: 0, blocked: 0, llmParsed: 0, skipped })
      return
    }

    const freshMessageIds = new Set(newMessages.map((msg) => String(msg?.platformMessageId || msg?.messageId || '').trim()).filter(Boolean))
    const freshParsedPrices = (parsed.prices || []).filter((row) => {
      const messageId = String(row?.messageId || '').trim()
      return !messageId || freshMessageIds.has(messageId)
    })

    if (!freshParsedPrices.length) {
      res.json({ ingested: newMessages.length, candidates: 0, prices: 0, signals: 0, lowConfidence: 0, blocked: 0, llmParsed: 0, skipped })
      return
    }

    const combinedPrices = [...freshParsedPrices]
    const uniquePrices = Array.from(
      new Map(combinedPrices.map((row) => [buildMemoryPriceDedupeKey(row), row])).values()
    )
    const { accepted, blocked } = sanitizeParsedPrices(uniquePrices)
    const persisted = await persistParsedPrices(accepted)

    res.json({
      ingested: newMessages.length,
      candidates: freshParsedPrices.length,
      prices: persisted.insertedPrices,
      signals: persisted.insertedSignals,
      lowConfidence: parsed.lowConfidence?.length || 0,
      blocked: blocked.length,
      llmParsed: 0,
      skipped
    })
  } catch (error) {
    res.status(500).json({ error: String(error) })
  }
})

app.post('/api/ingest', async (req, res) => {
  const { talker, talkers, start, end, limit, offset, keyword } = req.body || {}
  const targetTalkers = Array.isArray(talkers)
    ? talkers.filter(Boolean)
    : talker
      ? [talker]
      : []

  try {
    res.json(await runIngestJob({
      talkers: targetTalkers,
      start,
      end,
      limit,
      offset,
      keyword
    }))
  } catch (error) {
    const message = String(error)
    res.status(message.includes('Missing talker') ? 400 : message.includes('Upstream returned no messages') ? 502 : 500).json({ error: message })
  }
})

app.get('/api/source-settings', (_req, res) => {
  res.json(getSourceSettings())
})

app.post('/api/source-settings', async (req, res) => {
  try {
    const nextSettings = setSourceSettings(req.body || {})
    await persistSourceSettings()
    res.json(nextSettings)
  } catch (error) {
    res.status(500).json({ error: String(error) })
  }
})

app.get('/api/notification-settings', (_req, res) => {
  const settings = getNotificationSettings()
  res.json({
    ...settings,
    sendKey: settings.sendKey ? '***' : ''
  })
})

app.post('/api/notification-settings', async (req, res) => {
  try {
    const current = getNotificationSettings()
    const incoming = req.body || {}
    const nextSettings = setNotificationSettings({
      ...current,
      ...incoming,
      sendKey: incoming.sendKey ? String(incoming.sendKey) : current.sendKey,
      logs: Array.isArray(incoming.logs) ? incoming.logs : current.logs,
      lastDailyReportDate: incoming.lastDailyReportDate ?? current.lastDailyReportDate
    })
    await persistNotificationSettings()
    res.json({
      ...nextSettings,
      sendKey: nextSettings.sendKey ? '***' : ''
    })
  } catch (error) {
    res.status(500).json({ error: String(error) })
  }
})

app.get('/api/prices', async (req, res) => {
  const { talker, start, end, brand, type, favoritesOnly, activityRange } = req.query || {}
  const favoriteFilter = favoritesOnly === '1' || favoritesOnly === 'true'
  const rows = await listPrices({ talker, start, end, brand, type })
  const stats = await getPriceStats({ talker, start, end, brand })
  res.json({
    rows,
    stats,
    products: await listPriceTrendProducts({
      q: String(brand || ''),
      favoritesOnly: favoriteFilter,
      activityRange: String(activityRange || 'all')
    })
  })
})

app.get('/api/prices/product-detail', async (req, res) => {
  const { productUuid = '', brand = '', activityRange = 'all' } = req.query || {}
  const detail = await getPriceTrendDetail({
    productUuid: String(productUuid || ''),
    brand: String(brand || ''),
    activityRange: String(activityRange || 'all')
  })
  if (!detail) {
    res.status(404).json({ error: 'Product detail not found' })
    return
  }
  res.json(detail)
})

app.get('/api/signals', async (_req, res) => {
  res.json({ rows: await listSignals() })
})

app.get('/api/signals/config', async (_req, res) => {
  res.json(await getSignalConfig())
})

app.post('/api/signals/config', async (req, res) => {
  res.json(await updateSignalConfig(req.body || {}))
})

app.get('/api/products/:productUuid/signal-config', async (req, res) => {
  const productUuid = String(req.params.productUuid || '').trim()
  if (!productUuid) {
    res.status(400).json({ error: 'Missing productUuid' })
    return
  }
  const data = await listProductLibrary({ q: productUuid, limit: 1, offset: 0 })
  const row = (data.rows || []).find((item) => item.productUuid === productUuid)
  if (!row) {
    res.status(404).json({ error: 'Product not found' })
    return
  }
  res.json({
    productUuid,
    hasOverride: row.hasOverride,
    overrideConfig: row.overrideConfig || null,
    effectiveConfig: row.effectiveConfig
  })
})

app.post('/api/products/:productUuid/signal-config', async (req, res) => {
  const productUuid = String(req.params.productUuid || '').trim()
  if (!productUuid) {
    res.status(400).json({ error: 'Missing productUuid' })
    return
  }
  const payload = await updateProductSignalConfig(productUuid, req.body || {})
  await rebuildSignals()
  res.json(payload)
})

app.post('/api/signals/rebuild', async (_req, res) => {
  res.json(await rebuildSignals())
})

app.post('/api/signals', async (req, res) => {
  const { signals } = req.body || {}
  if (!Array.isArray(signals)) {
    res.status(400).json({ error: 'signals must be array' })
    return
  }
  await insertSignals(signals)
  res.json({ inserted: signals.length })
})

app.get('/api/aliases', async (_req, res) => {
  res.json({ rows: await listAliases() })
})

app.get('/api/aliases/pending', async (_req, res) => {
  res.json({ rows: await listPendingAliases() })
})

app.post('/api/aliases/:id/confirm', async (req, res) => {
  const record = await confirmPendingAlias(req.params.id, req.body || {})
  if (!record) {
    res.status(404).json({ error: 'Pending alias not found' })
    return
  }
  res.json({ row: record })
})

app.post('/api/aliases/:id/reject', async (req, res) => {
  const record = await rejectPendingAlias(req.params.id, String(req.body?.notes || ''))
  if (!record) {
    res.status(404).json({ error: 'Pending alias not found' })
    return
  }
  res.json({ row: record })
})

app.post('/api/aliases/:alias/update', async (req, res) => {
  const alias = decodeURIComponent(req.params.alias || '')
  const canonical = String(req.body?.canonical || '').trim()
  if (!alias || !canonical) {
    res.status(400).json({ error: 'Missing alias or canonical' })
    return
  }
  const row = await upsertAlias({
    alias,
    canonical,
    confidence: Number(req.body?.confidence) || 1,
    source: req.body?.source || 'manual-edit',
    notes: String(req.body?.notes || '')
  })
  res.json({ row })
})

app.delete('/api/aliases/:alias', async (req, res) => {
  const alias = decodeURIComponent(req.params.alias || '')
  const deleted = await deleteAlias(alias)
  if (!deleted) {
    res.status(404).json({ error: 'Alias not found' })
    return
  }
  res.json({ deleted: true })
})

app.get('/api/products/search', async (req, res) => {
  const { q = '', limit, offset } = req.query || {}
  const parsedLimit = limit == null || limit === '' ? undefined : Math.max(1, Number(limit) || 20)
  const parsedOffset = Math.max(0, Number(offset) || 0)
  res.json({ rows: await listProducts({ q: String(q || ''), limit: parsedLimit, offset: parsedOffset }) })
})

app.get('/api/products/library', async (req, res) => {
  const { q = '', limit, offset, favoritesOnly, activityRange = 'all' } = req.query || {}
  const parsedLimit = limit == null || limit === '' ? 10 : Math.max(1, Number(limit) || 10)
  const parsedOffset = Math.max(0, Number(offset) || 0)
  const favoriteFilter = favoritesOnly === '1' || favoritesOnly === 'true'
  res.json(await listProductLibrary({
    q: String(q || ''),
    limit: parsedLimit,
    offset: parsedOffset,
    favoritesOnly: favoriteFilter,
    activityRange: String(activityRange || 'all')
  }))
})

app.get('/api/products/favorites', async (_req, res) => {
  res.json({ rows: await listFavoriteProducts() })
})

app.post('/api/products/:productUuid/favorite', async (req, res) => {
  const productUuid = String(req.params.productUuid || '').trim()
  if (!productUuid) {
    res.status(400).json({ error: 'Missing productUuid' })
    return
  }
  res.json(await setProductFavorite(productUuid, req.body?.favorite !== false))
})

app.post('/api/aliases/reapply', async (_req, res) => {
  res.json(await reapplyAliasMappings())
})

let listenPort = 5403
let scheduledTimer = null
let httpServer = null
let startupPromise = null

async function listenWithRetry(startPort = 5403, maxTries = 20) {
  for (let i = 0; i < maxTries; i += 1) {
    const port = startPort + i
    try {
      const server = await new Promise((resolve, reject) => {
        const instance = app.listen(port, () => resolve(instance))
        instance.on('error', reject)
      })
      listenPort = port
      httpServer = server
      console.log(`[server] listening on ${port}`)
      return port
    } catch (error) {
      if (error?.code !== 'EADDRINUSE') throw error
    }
  }
  throw new Error('No available port found')
}

export async function startServer() {
  if (httpServer) {
    return { port: listenPort, server: httpServer }
  }
  if (startupPromise) {
    return startupPromise
  }

  startupPromise = (async () => {
    await loadConfigFile()
    await initDb()
    await listenWithRetry()
    scheduledTimer = setInterval(() => {
      tickScheduledIngest().catch((error) => {
        console.error('[schedule] tick failed', error)
      })
      tickDailyReport().catch((error) => {
        console.error('[schedule] daily report failed', error)
      })
    }, 60 * 1000)
    tickScheduledIngest().catch((error) => {
      console.error('[schedule] initial tick failed', error)
    })
    tickDailyReport().catch((error) => {
      console.error('[schedule] initial daily report failed', error)
    })
    return { port: listenPort, server: httpServer }
  })()

  try {
    return await startupPromise
  } finally {
    startupPromise = null
  }
}

export async function stopServer() {
  if (scheduledTimer) {
    clearInterval(scheduledTimer)
    scheduledTimer = null
  }

  if (!httpServer) return

  const server = httpServer
  httpServer = null
  await new Promise((resolve, reject) => {
    server.close((error) => {
      if (error) reject(error)
      else resolve()
    })
  })
}

const isDirectRun = process.argv[1] && path.resolve(process.argv[1]) === __filename
if (isDirectRun) {
  startServer().catch((error) => {
    console.error('[server] failed to start', error)
    process.exit(1)
  })
}
