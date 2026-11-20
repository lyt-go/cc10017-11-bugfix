<template>
  <section class="page" data-module="change_capture">
    <header class="page-head">
      <div><h2>CDC捕获</h2><p class="page-desc">维护数据库CDC的日志捕获、快照与增量切换配置，为CDC捕获提供可验证的数据工程流程。</p></div>
      <div class="page-actions">
        <button class="btn primary" type="button" data-testid="open-create" @click="openCreate">登记CDC配置</button>
        <button class="btn" type="button" data-testid="export" @click="exportRows">导出作业清单</button>
      </div>
    </header>
    <div class="filter-bar">
      <label><span>开始日期</span><input type="date" v-model="dateFrom" /></label>
      <label><span>结束日期</span><input type="date" v-model="dateTo" /></label>
      <button class="btn" type="button" @click="reload">查询</button>
    </div>
    <div class="timeline-wrap">
      <div v-for="item in timelineItems" :key="item.id" class="timeline-row">
        <span class="timeline-label">{{ item.name || item.label }}</span>
        <div class="timeline-track">
          <div class="timeline-bar" :class="`status-${item.status}`" :style="{ left: barLeft(item) + '%', width: barWidth(item) + '%' }" :data-testid="`bar-${item.id}`" :title="`${item.start} ~ ${item.end}`" @click="selectItem(item)">&nbsp;</div>
        </div>
        <span style="font-size:12px;color:var(--muted);margin-left:8px;">{{ item.start }} ~ {{ item.end }}</span>
      </div>
      <p v-if="!timelineItems.length" style="text-align:center;color:var(--muted)">暂无作业记录</p>
    </div>
    <div v-if="selectedItem" class="timeline-log" data-testid="timeline-log">
      <strong>{{ selectedItem.name || selectedItem.label }}</strong> — 状态：{{ selectedItem.status }}
      <div style="margin-top:4px;">{{ selectedItem.logs || '暂无日志' }}</div>
    </div>

    <section class="snapshot-panel cdc-snapshot" data-testid="cdc-snapshot">
      <header class="snapshot-head">
        <strong>终态合并数据快照核对</strong>
        <label class="filter-item">
          <span>CDC配置</span>
          <select v-model="snapshotConfigId" data-testid="snapshot-config">
            <option v-for="cdc in allItems" :key="cdc.id" :value="cdc.id">{{ cdc.name }} · {{ cdc.status }}</option>
          </select>
        </label>
        <button class="btn" type="button" data-testid="run-capture-demo" @click="runCaptureDemo">运行两批CDC变更</button>
        <button class="btn" type="button" data-testid="verify-snapshot" @click="loadSnapshot">核对删除序号</button>
      </header>
      <p v-if="!allItems.length" class="empty-state">暂无可核对的CDC配置</p>
      <template v-else-if="snapshot">
        <p class="snapshot-foot" data-testid="cdc-verify">
          快照版本 {{ snapshot.snapshot_version }} · 下游版本 {{ snapshot.downstream_version }} ·
          删除序号 {{ deleteSeqText }} ·
          <span :class="snapshot.consistency?.consistent ? 'status-ok' : 'status-bad'" data-testid="cdc-consistency">
            {{ snapshot.consistency?.consistent ? '快照与下游同版本一致' : snapshot.consistency?.reason }}
          </span>
        </p>
        <p v-if="snapshot.pending_publication" class="snapshot-pending" data-testid="cdc-pending">
          发布中断：版本 {{ snapshot.pending_publication.version }} 停在 {{ snapshot.pending_publication.stage }}，
          可回到增量合并页重试补齐。
        </p>
        <table class="data-table snapshot-table">
          <thead>
            <tr><th>合并键</th><th>业务标识</th><th>删除标记</th><th>最终排序值</th><th>变更序号</th><th>源表版本</th><th>来源批次</th></tr>
          </thead>
          <tbody>
            <tr v-for="item in snapshot.items" :key="String(item.merge_key)" :data-testid="`cdc-snapshot-row-${item.merge_key}`" :data-deleted="String(item.is_deleted)">
              <td>{{ item.merge_key }}</td>
              <td>{{ item['业务标识'] }}</td>
              <td>{{ item.is_deleted ? '已删除' : '有效' }}</td>
              <td>{{ item['最终排序值'] ?? '—' }}</td>
              <td>{{ item['变更序号'] ?? '—' }}</td>
              <td>{{ item['源表版本'] ?? '—' }}</td>
              <td>{{ item['来源批次'] ?? '—' }}</td>
            </tr>
          </tbody>
        </table>
        <p class="snapshot-foot" data-testid="cdc-downstream-visible">
          下游最终可见：{{ downstreamVisible.length ? downstreamVisible.join('、') : '无（全部为删除终态）' }}
        </p>
      </template>
      <p v-else class="empty-state">选择CDC配置后可核对它写入的终态合并数据快照</p>
      <p v-if="errorMessage" class="error-text">{{ errorMessage }}</p>
    </section>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate"><h3>登记CDC配置</h3>
        <label v-for="field in columns.slice(0,3)" :key="field" class="create-field"><span>{{ field }}</span><input v-model="createValues[field]" :placeholder="`请输入${field}`" required /></label>
        <div style="display:flex;gap:8px;justify-content:flex-end;margin-top:12px">
          <button class="btn" type="button" @click="showCreate=false">取消</button>
          <button class="btn primary" type="submit" data-testid="submit-create">提交</button>
        </div>
      </form>
    </div>
  </section>
</template>
<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'
import { request } from '@/api/client'
const ENDPOINT = '/api/change_capture'
const columns = ["配置编号", "源数据库", "源表名", "快照模式", "捕获列", "变更序号", "增量阶段", "目标主题"]
const actions = ["提交校验", "启动捕获", "停止捕获"]
const timelineItems = ref<any[]>([])
const selectedItem = ref<any>(null)
const dateFrom = ref(''); const dateTo = ref('')
const errorMessage = ref(''); const showCreate = ref(false)
const createValues = ref<Record<string,string>>({})
const allItems = ref<any[]>([])
const snapshotConfigId = ref<number | null>(null)
const snapshot = ref<Record<string, any> | null>(null)
const deleteSeqText = computed(() => {
  const deleted = (snapshot.value?.deleted ?? []) as any[]
  if (!deleted.length) return '无删除终态'
  return deleted.map((item) => `${item.merge_key}=${item['变更序号'] ?? '—'}`).join('、')
})
const downstreamVisible = computed(() =>
  ((snapshot.value?.downstream?.visible ?? []) as any[]).map((item) => String(item.merge_key)),
)
function barLeft(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 0; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return ((new Date(item.start).getTime() - min) / range) * 100 }
function barWidth(item: any) { const all = allItems.value.filter(i => i.start && i.end); if(!all.length) return 20; const min = Math.min(...all.map(i => new Date(i.start).getTime())); const max = Math.max(...all.map(i => new Date(i.end).getTime())); const range = max - min || 86400000; return Math.max(5, ((new Date(item.end).getTime() - new Date(item.start).getTime()) / range) * 100) }
function selectItem(item: any) { selectedItem.value = selectedItem.value?.id === item.id ? null : item }
async function reload() {
  try {
    const res = await request(ENDPOINT)
    const data = await res.json()
    allItems.value = data.items || []
    applyFilter()
    if (snapshotConfigId.value === null && allItems.value.length) {
      snapshotConfigId.value = allItems.value[0].id
    }
    if (snapshotConfigId.value !== null) await loadSnapshot()
  } catch(e) { errorMessage.value = '加载失败' }
}

async function loadSnapshot() {
  if (snapshotConfigId.value === null) return
  errorMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${snapshotConfigId.value}/snapshot`)
    if (!res.ok) throw new Error(res.status === 404 ? '该CDC配置未关联增量合并策略' : '终态快照读取失败')
    snapshot.value = await res.json()
  } catch (e: any) {
    snapshot.value = null
    errorMessage.value = e.message
  }
}

async function runCaptureDemo() {
  if (snapshotConfigId.value === null) return
  errorMessage.value = ''
  try {
    const res = await request(`${ENDPOINT}/${snapshotConfigId.value}/capture-demo`, { method: 'POST' })
    const payload = await res.json()
    if (!res.ok || !payload.ok) throw new Error(payload.message || 'CDC变更写入失败')
    await reload()
  } catch (e: any) {
    errorMessage.value = e.message
  }
}
function applyFilter() {
  let items = allItems.value
  if(dateFrom.value) items = items.filter((i: any) => i.start >= dateFrom.value)
  if(dateTo.value) items = items.filter((i: any) => i.end <= dateTo.value)
  timelineItems.value = items; selectedItem.value = null
}
function openCreate() { createValues.value = {}; showCreate.value = true }
async function submitCreate() {
  try { const res = await request(ENDPOINT, { method: 'POST', body: JSON.stringify({ values: createValues.value }) }); if(!res.ok) throw new Error('创建失败'); showCreate.value = false; await reload() } catch(e: any) { errorMessage.value = e.message }
}
function exportRows() { window.open(`${ENDPOINT}/export`, '_blank') }
onMounted(reload)
</script>
