<template>
  <section class="page" data-module="upsert_merge">
    <header class="page-head">
      <div>
        <h2>增量合并管理</h2>
        <p class="page-desc">维护流式增量数据的UPSERT、DELETE与Compaction策略，为增量合并提供可验证的数据工程流程。</p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="run-merge-demo" type="button" @click="runDemoFirst">运行两批变更</button>
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记合并策略</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出增量合并清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in columns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <template v-for="row in rows" :key="String(row.id)">
        <tr :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in columns" :key="column">{{ row[column] ?? '—' }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
            <button class="link" type="button" :data-testid="`snapshot-open-${row.id}`" @click="loadSnapshot(row)">终态快照</button>
          </td>
        </tr>
        <tr class="snapshot-detail-row" v-if="snapshot && snapshot.policy_id === Number(row.id)">
          <td :colspan="columns.length + 1">
            <div class="snapshot-panel" :data-testid="`snapshot-${row.id}`" :data-consistent="String(!!snapshot.consistency?.consistent)">
              <header class="snapshot-head">
                <strong>终态合并数据快照</strong>
                <span :data-testid="`snapshot-version-${row.id}`">快照版本 {{ snapshot.snapshot_version }} · 下游版本 {{ snapshot.downstream_version }}</span>
                <span :class="snapshot.consistency?.consistent ? 'status-ok' : 'status-bad'" :data-testid="`snapshot-consistency-${row.id}`">
                  {{ snapshot.consistency?.consistent ? '快照与下游同版本一致' : snapshot.consistency?.reason }}
                </span>
                <button class="btn ghost" type="button" data-testid="refresh-snapshot" @click="loadSnapshot(row)">刷新快照</button>
                <button class="btn ghost" type="button" data-testid="retry-snapshot" @click="retrySnapshot(row)">重试补齐</button>
                <button class="btn ghost" type="button" data-testid="close-snapshot" @click="snapshot = null">收起</button>
              </header>
              <div v-if="snapshot.pending_publication" class="snapshot-pending" data-testid="snapshot-pending">
                发布中断：版本 {{ snapshot.pending_publication.version }} 停在 {{ snapshot.pending_publication.stage }}，
                重试会从已写入的终态快照补齐下游记录。
              </div>
              <table class="data-table snapshot-table">
                <thead>
                  <tr>
                    <th>合并键</th>
                    <th>业务标识</th>
                    <th>删除标记</th>
                    <th>最终排序值</th>
                    <th>变更序号</th>
                    <th>源表版本</th>
                    <th>来源批次</th>
                    <th>快照版本</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="item in snapshot.items" :key="String(item.merge_key)" :data-testid="`snapshot-row-${item.merge_key}`" :data-deleted="String(item.is_deleted)">
                    <td>{{ item.merge_key }}</td>
                    <td>{{ item['业务标识'] }}</td>
                    <td>{{ item.is_deleted ? '已删除' : '有效' }}</td>
                    <td>{{ item['最终排序值'] ?? '—' }}</td>
                    <td>{{ item['变更序号'] ?? '—' }}</td>
                    <td>{{ item['源表版本'] ?? '—' }}</td>
                    <td>{{ item['来源批次'] ?? '—' }}</td>
                    <td>{{ item['快照版本'] }}</td>
                  </tr>
                  <tr v-if="!snapshot.items.length">
                    <td :colspan="8" class="empty-state">暂无快照记录，可先运行两批变更</td>
                  </tr>
                </tbody>
              </table>
              <p class="snapshot-foot" data-testid="snapshot-summary">
                共 {{ snapshot.record_count }} 条 · 已删除 {{ snapshot.deleted_count }} 条 ·
                下游可见 {{ snapshot.downstream?.items?.filter((item: any) => !item.is_deleted).length ?? 0 }} 条
              </p>
            </div>
          </td>
        </tr>
        </template>
        <tr v-if="!rows.length">
          <td :colspan="columns.length + 1" class="empty-state">暂无增量合并数据，可先登记合并策略</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记合并策略</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条增量合并记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/upsert_merge'
const columns = ["策略编号", "目标表名", "合并键列", "插入列", "更新列", "删除标记", "排序字段", "合并引擎"]
const actions = ["提交校验", "标记生效", "停用策略"]
const statuses = ["草稿", "待校验", "已生效", "已停用"]
const metricNames = ["合并策略总数", "待校验策略", "已生效策略"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const snapshot = ref<Record<string, any> | null>(null)
const snapshotLoading = ref(false)
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === statuses[1]).length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === statuses[statuses.length - 1]).length },
])

function resetFilters() {
  filters.value = {}
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'upsert_merge.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('增量合并动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '增量合并操作失败'
  }
}

async function loadSnapshot(row: Row) {
  errorMessage.value = ''
  snapshotLoading.value = true
  try {
    const response = await request(`${ENDPOINT}/${row.id}/snapshot`)
    if (!response.ok) throw new Error('终态合并数据快照读取失败')
    const payload = await response.json()
    snapshot.value = { ...payload, policy_id: Number(row.id) }
  } catch (error) {
    snapshot.value = null
    errorMessage.value = error instanceof Error ? error.message : '终态快照读取失败'
  } finally {
    snapshotLoading.value = false
  }
}

async function runDemo(row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/merge-demo`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '两批变更合并失败')
    await loadSnapshot(row)
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '增量合并失败'
  }
}

async function runDemoFirst() {
  const target = rows.value.find((row) => row.status === statuses[2]) ?? rows.value[0]
  if (!target) {
    errorMessage.value = '没有可运行的合并策略，请先登记并标记生效'
    return
  }
  await runDemo(target)
}

async function retrySnapshot(row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/snapshot/retry`, { method: 'POST' })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '终态快照补齐失败')
    await loadSnapshot(row)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '终态快照补齐失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(filters.value as Record<string, string>).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('合并策略列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '增量合并列表读取失败'
  }
}

onMounted(reload)
</script>
