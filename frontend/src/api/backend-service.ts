import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  DeliveryManifest,
  DeliverySummary,
  DeliveryTraceResult,
  EntryRow,
  OverviewResult,
  PageResult,
} from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---------- 行级来源链与来源清单 ----------

export async function listDeliveries(): Promise<{ items: DeliverySummary[]; total: number }> {
  const response = await request('/api/pipeline/deliveries')
  if (!response.ok) throw new Error('来源清单读取失败')
  return await response.json()
}

export async function getDelivery(exportId: number): Promise<DeliveryManifest> {
  const response = await request(`/api/pipeline/deliveries/${exportId}`)
  if (!response.ok) throw new Error('交付明细读取失败')
  return await response.json()
}

export async function verifyDelivery(exportId: number): Promise<{
  ok: boolean
  message: string
  manifest: DeliveryManifest
}> {
  const response = await request(`/api/pipeline/deliveries/${exportId}/verify`, {
    method: 'POST',
  })
  if (!response.ok) throw new Error('文件与来源核验失败')
  return await response.json()
}

export async function traceDelivery(
  exportId: number,
  params: { group?: string; batch_no?: string; slice_id?: string } = {},
): Promise<DeliveryTraceResult> {
  const query = new URLSearchParams(
    Object.fromEntries(Object.entries(params).filter(([, value]) => value)),
  ).toString()
  const response = await request(`/api/pipeline/deliveries/${exportId}/trace?${query}`)
  if (!response.ok) throw new Error('来源追溯读取失败')
  return await response.json()
}

export function deliveryDownloadUrl(exportId: number): string {
  return `/api/pipeline/deliveries/${exportId}/download`
}

/** 下载交付文件：只有文件与来源同时核验通过后后端才返回内容。 */
export async function downloadDelivery(exportId: number, fileName: string): Promise<void> {
  const response = await request(deliveryDownloadUrl(exportId))
  if (!response.ok) {
    const detail = await response.json().catch(() => null)
    throw new Error(detail?.detail ?? '交付文件尚未通过核验，不能下载')
  }
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = fileName
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

/** 对一个聚合视图生成导出，流水线会同时固化行级来源切片与来源清单。 */
export async function buildExport(payload: {
  view_name: string
  export_format?: string
  batch_no?: string
  dim_field?: string
}): Promise<{ ok: boolean; message: string; export: { id: number }; manifest: DeliveryManifest }> {
  const response = await request('/api/pipeline/export/build', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  if (!response.ok) throw new Error('生成导出失败')
  return await response.json()
}
