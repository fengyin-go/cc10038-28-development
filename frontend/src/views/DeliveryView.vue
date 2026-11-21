<template>
  <section class="page delivery-page">
    <header class="page-head">
      <div>
        <h2>交付来源清单</h2>
        <p class="page-desc">
          按分组核对每次交付的文件行、来源链和原始正文；文件与来源同时核验通过后才能对外交付。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-deliveries" type="button" @click="reload">
          刷新清单
        </button>
      </div>
    </header>

    <form class="filter-bar" @submit.prevent="generate">
      <label class="filter-item">
        <span>聚合视图</span>
        <input
          v-model="buildView"
          data-testid="build-view"
          placeholder="如 view-INGE-0004"
        />
      </label>
      <label class="filter-item">
        <span>批次（可选）</span>
        <input v-model="buildBatch" data-testid="build-batch" placeholder="如 INGE-0004" />
      </label>
      <label class="filter-item">
        <span>分组字段（无视图时）</span>
        <input v-model="buildDim" data-testid="build-dim" placeholder="level" />
      </label>
      <button class="btn primary" data-testid="build-export" type="submit" :disabled="building">
        {{ building ? '生成中…' : '生成交付文件' }}
      </button>
    </form>

    <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>

    <table class="data-table">
      <thead>
        <tr>
          <th>文件</th>
          <th>导出范围</th>
          <th>公开行数</th>
          <th>等待核验</th>
          <th>异常切片</th>
          <th>分组数</th>
          <th>文件摘要</th>
          <th>来源摘要</th>
          <th>交付状态</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="item in deliveries"
          :key="item.id"
          :data-testid="`delivery-${item.id}`"
          :data-status="item.交付状态"
          :class="{ selected: item.id === selectedId }"
        >
          <td>{{ item.文件名称 }}</td>
          <td>{{ item.导出范围 }}</td>
          <td>{{ item.文件行数 }}</td>
          <td>{{ item.等待切片数 }}</td>
          <td>{{ item.异常切片数 }}</td>
          <td>{{ item.分组数 }}</td>
          <td class="mono">{{ item.文件摘要 }}</td>
          <td class="mono">{{ item.来源摘要 }}</td>
          <td>
            <span class="badge" :data-state="item.交付状态">{{ item.交付状态 }}</span>
            <span v-if="item.核验状态 !== '已核验'" class="muted">/ {{ item.核验状态 }}</span>
          </td>
          <td class="row-actions">
            <button class="link" type="button" :data-testid="`open-${item.id}`" @click="open(item.id)">
              查看
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`verify-${item.id}`"
              @click="verify(item.id)"
            >
              核验
            </button>
            <button
              class="link"
              type="button"
              :data-testid="`download-${item.id}`"
              :disabled="item.交付状态 !== '可交付'"
              @click="download(item)"
            >
              下载
            </button>
          </td>
        </tr>
        <tr v-if="!deliveries.length">
          <td colspan="10" class="empty-state">暂无交付文件，先执行一次导出生成来源清单</td>
        </tr>
      </tbody>
    </table>

    <section v-if="manifest" class="delivery-detail" data-testid="delivery-detail">
      <header class="detail-head">
        <h3>文件 {{ manifest.文件名称 }} 的分组来源清单</h3>
        <span class="badge" :data-state="manifest.交付状态">{{ manifest.交付状态 }}</span>
      </header>

      <table class="data-table">
        <thead>
          <tr>
            <th>分组</th>
            <th>记录数</th>
            <th>公开</th>
            <th>等待</th>
            <th>异常</th>
            <th>版本冲突</th>
            <th>来源摘要</th>
            <th>分组状态</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="([groupName, group]) in groupRows"
            :key="groupName"
            :data-testid="`group-${groupName}`"
          >
            <td>{{ groupName }}</td>
            <td>{{ group.记录数 }}</td>
            <td>{{ group.公开记录数 }}</td>
            <td>{{ group.等待记录数 }}</td>
            <td>{{ group.异常记录数 }}</td>
            <td>
              <span v-if="group.版本冲突" class="warn-text">存在版本冲突（按各自版本处理）</span>
              <span v-else>—</span>
            </td>
            <td class="mono">{{ group.来源摘要 }}</td>
            <td>{{ group.状态 }}</td>
            <td>
              <button
                class="link"
                type="button"
                :data-testid="`trace-${groupName}`"
                @click="traceGroup(groupName)"
              >
                追溯输入
              </button>
            </td>
          </tr>
        </tbody>
      </table>

      <h4>公开文件行</h4>
      <table class="data-table">
        <thead>
          <tr>
            <th>来源链ID</th>
            <th>分组</th>
            <th>批次</th>
            <th>原始记录</th>
            <th>明细ID</th>
            <th>采集版本</th>
            <th>解析规则</th>
            <th>解析版本</th>
            <th>标准字段</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in manifest.文件行" :key="row.lineage_id">
            <td class="mono">{{ row.source_lineage_id }}</td>
            <td>{{ row.group }}</td>
            <td>{{ row.batch_no }}</td>
            <td>{{ row.raw_record_id }}</td>
            <td>{{ row.parsed_entity_id }}</td>
            <td>v{{ row.source_version }}</td>
            <td>{{ row.parser_rule }}</td>
            <td>v{{ row.parser_version }}</td>
            <td class="mono">{{ JSON.stringify(row.fields) }}</td>
          </tr>
          <tr v-if="!manifest.文件行.length">
            <td colspan="9" class="empty-state">没有已核验的公开行，等待中的日志仍按自己的版本等待复验</td>
          </tr>
        </tbody>
      </table>

      <div v-if="trace" class="trace-panel" data-testid="trace-panel">
        <h4>分组「{{ traceGroupName }}」的输入明细与原始正文</h4>
        <p v-if="!trace.trace.length" class="empty-state">该分组没有可追溯的切片</p>
        <article v-for="item in trace.trace" :key="item.切片ID" class="trace-item">
          <dl class="dag-detail">
            <dt>切片ID / 来源链</dt>
            <dd>{{ item.切片ID }} / {{ item.来源链ID }}</dd>
            <dt>核验状态</dt>
            <dd>{{ item.核验状态 }}</dd>
            <dt>采集版本 / 解析版本</dt>
            <dd>v{{ item.采集版本 }} / v{{ item.解析版本 }}</dd>
            <dt>版本冲突</dt>
            <dd>{{ item.版本冲突 ? item.冲突说明 : '无' }}</dd>
            <dt>原始正文</dt>
            <dd class="mono">{{ item.原始日志?.原始正文 ?? '缺失' }}</dd>
            <dt>正文 / 明细摘要一致</dt>
            <dd>{{ item.原始正文一致 ? '是' : '否' }} / {{ item.标准化摘要一致 ? '是' : '否' }}</dd>
            <dt>采集时来源配置</dt>
            <dd class="mono">{{ item.采集时来源配置?.配置快照 ?? '缺失' }}</dd>
            <dt>解析时规则配置</dt>
            <dd class="mono">{{ item.解析时规则配置?.配置快照 ?? '缺失' }}</dd>
            <dt>采集批次</dt>
            <dd>{{ item.采集批次?.批次编号 ?? '缺失' }}（{{ item.采集批次?.所属数据源 ?? '—' }}）</dd>
            <dt>标准化明细</dt>
            <dd class="mono">{{ item.标准化明细?.标准化字段集 ?? '缺失' }}</dd>
          </dl>
        </article>
      </div>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  buildExport,
  downloadDelivery,
  getDelivery,
  listDeliveries,
  traceDelivery,
  verifyDelivery,
} from '@/api/backend-service'
import type {
  DeliveryManifest,
  DeliverySummary,
  DeliveryTraceResult,
} from '@/data/types'

const deliveries = ref<DeliverySummary[]>([])
const manifest = ref<DeliveryManifest | null>(null)
const trace = ref<DeliveryTraceResult | null>(null)
const traceGroupName = ref('')
const selectedId = ref<number | null>(null)
const errorMessage = ref('')
const buildView = ref('')
const buildBatch = ref('')
const buildDim = ref('level')
const building = ref(false)

const groupRows = computed(() =>
  Object.entries(manifest.value?.分组 ?? {}).sort(([left], [right]) =>
    left.localeCompare(right),
  ),
)

async function reload() {
  errorMessage.value = ''
  try {
    const payload = await listDeliveries()
    deliveries.value = payload.items
    if (
      manifest.value &&
      !payload.items.some((item) => item.id === manifest.value?.id)
    ) {
      manifest.value = null
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '来源清单读取失败'
  }
}

async function open(exportId: number) {
  errorMessage.value = ''
  try {
    selectedId.value = exportId
    manifest.value = await getDelivery(exportId)
    trace.value = null
    traceGroupName.value = ''
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '交付明细读取失败'
  }
}

async function verify(exportId: number) {
  errorMessage.value = ''
  try {
    const result = await verifyDelivery(exportId)
    await reload()
    if (selectedId.value === exportId || selectedId.value === null) {
      await open(exportId)
    }
    if (!result.ok) {
      errorMessage.value = result.message
    }
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '核验失败'
  }
}

async function traceGroup(groupName: string) {
  if (!manifest.value) return
  errorMessage.value = ''
  try {
    trace.value = await traceDelivery(manifest.value.id, { group: groupName })
    traceGroupName.value = groupName
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '来源追溯失败'
  }
}

async function download(item: DeliverySummary) {
  errorMessage.value = ''
  try {
    await downloadDelivery(item.id, item.文件名称)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '下载失败'
  }
}

async function generate() {
  errorMessage.value = ''
  building.value = true
  try {
    const result = await buildExport({
      view_name: buildView.value.trim(),
      batch_no: buildBatch.value.trim() || undefined,
      dim_field: buildDim.value.trim() || undefined,
    })
    await reload()
    await open(result.export.id)
    errorMessage.value = result.manifest.交付状态 === '可交付'
      ? ''
      : `文件 ${result.export.id} 已生成：${result.manifest.交付状态}，请核验后再下载`
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '生成导出失败'
  } finally {
    building.value = false
  }
}

onMounted(reload)
</script>

<style scoped>
.delivery-detail {
  margin-top: 20px;
  padding-top: 12px;
  border-top: 2px solid var(--border);
}
.detail-head {
  display: flex;
  align-items: center;
  gap: 12px;
}
.badge {
  display: inline-block;
  border-radius: 999px;
  padding: 2px 10px;
  font-size: 12px;
  border: 1px solid var(--border);
  background: #f8fafc;
}
.badge[data-state='可交付'] {
  border-color: #22c55e;
  color: #15803d;
  background: #f0fdf4;
}
.badge[data-state='待核验'] {
  border-color: #f59e0b;
  color: #b45309;
  background: #fffbeb;
}
.badge[data-state='交付阻断'],
.badge[data-state='核验异常'] {
  border-color: #ef4444;
  color: #b42318;
  background: #fef2f2;
}
.mono {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
  word-break: break-all;
}
.muted {
  color: var(--muted);
  font-size: 12px;
}
.warn-text {
  color: #b45309;
}
tr.selected td {
  background: #eff6ff;
}
.trace-panel {
  margin-top: 16px;
}
.trace-item {
  border: 1px solid var(--border);
  border-radius: 8px;
  margin-bottom: 10px;
  background: #fff;
}
</style>
