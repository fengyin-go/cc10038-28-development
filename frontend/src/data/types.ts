/** 纯前端数据层的公共类型：与全栈版后端返回的结构保持一致，换回后端时页面不用改。 */

export type EntryRow = {
  id: number
  status: string
  pending: boolean
  abnormal: boolean
  [field: string]: string | number | boolean
}

export type ModuleMeta = {
  key: string
  name: string
  entity: string
  desc: string
  fields: string[]
  statuses: string[]
  actions: string[]
  actionTargets: Record<string, string>
  metrics: string[]
}

export type PageResult = {
  items: EntryRow[]
  total: number
  page: number
  size: number
}

export type ActionResult = {
  ok: boolean
  message: string
}

export type DeliveryGroup = {
  分组值: string
  聚合维度: string
  记录数: number
  公开记录数: number
  等待记录数: number
  异常记录数: number
  指标合计: number
  状态: string
  是否公开: boolean
  版本冲突?: boolean
  文件摘要?: string
  来源摘要?: string
}

export type DeliverySummary = {
  id: number
  文件名称: string
  导出范围: string
  目标格式: string
  文件行数: number
  公开切片数: number
  等待切片数: number
  异常切片数: number
  分组数: number
  文件摘要: string
  来源摘要: string
  核验状态: string
  交付状态: string
  生成时间: string
  核验时间: string
}

export type DeliverySlice = {
  id: number
  来源行ID: string
  分组值: string
  来源批次: string
  数据源: string
  原始记录ID: number
  标准化记录ID: number
  原始行号: string
  采集版本: number
  解析规则: string
  解析版本: number
  原始正文摘要: string
  标准化摘要: string
  核验状态: string
  版本冲突: boolean
  冲突说明: string
  核验说明: string
}

export type DeliveryManifest = DeliverySummary & {
  分组: Record<string, DeliveryGroup>
  文件行: DeliveryFileRow[]
  切片: DeliverySlice[]
}

export type DeliveryFileRow = {
  lineage_id: number
  source_lineage_id: string
  group: string
  batch_no: string
  raw_record_id: number
  parsed_entity_id: number
  source_version: number
  parser_rule: string
  parser_version: number
  fields: Record<string, string>
}

export type DeliveryTrace = {
  分组值: string
  切片ID: number
  来源链ID: string
  核验状态: string
  版本冲突: boolean
  冲突说明: string
  采集版本: number
  解析版本: number
  原始正文摘要: string
  标准化摘要: string
  原始正文一致: boolean
  标准化摘要一致: boolean
  采集批次: Record<string, string | number | boolean> | null
  标准化明细: Record<string, string | number | null> | null
  原始日志: Record<string, string | number | null> | null
  采集时来源配置: { 版本标签?: string; 配置快照?: string; 连接凭证摘要?: string } | null
  解析时规则配置: { 版本标签?: string; 配置快照?: string } | null
}

export type DeliveryTraceResult = {
  export_id: number
  文件名称: string
  核验状态: string
  交付状态: string
  trace: DeliveryTrace[]
}

export type OverviewResult = {
  cards: { label: string; value: number }[]
  modules: { name: string; created: number; pending: number; abnormal: number }[]
}
