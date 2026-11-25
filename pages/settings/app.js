const bridge = window.AstrBotPluginView || window.AstrBotPluginPage;
const field = id => document.getElementById(id);
const mib = 1024 * 1024;
let saved;
let used = 0;
let selected = new Set();
let directory = [];
let archived = [];
let discovering = false;

function notice(id, message = "", error = false) {
  field(id).textContent = message;
  field(id).classList.toggle("error", error);
}

function size(bytes) {
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let index = 0;
  while (bytes >= 1024 && index < units.length - 1) {
    bytes /= 1024;
    index++;
  }
  return bytes.toLocaleString("zh-CN", {maximumFractionDigits: 1}) + " " + units[index];
}

function retention() {
  const rolling = field("rolling").checked;
  const capacity = Number(field("capacity").value);
  const pruneNow = rolling && capacity > 0 && used > capacity * mib;
  const text = pruneNow ? "当前用量超过新上限，保存后会立即清理最旧的消息。"
    : rolling ? "空间不足时，从所有归档群中按消息时间删除最旧记录。"
      : "空间不足时保留已有记录，跳过放不下的新消息。";
  notice("retention", text, pruneNow);
}

function renderGroups() {
  const rows = field("groupRows");
  rows.replaceChildren();
  const groups = new Map(directory.map(group => [group.group_id, group]));
  // A disconnected account must not turn a saved selection into an unchecked group.
  for (const id of [...(saved?.group_ids || []), ...selected]) {
    if (!groups.has(id)) groups.set(id, {group_id: id, online: false});
  }
  const search = field("groupSearch").value.trim().toLocaleLowerCase();
  for (const group of groups.values()) {
    const id = group.group_id, name = group.group_name || "群名待获取";
    if (search && !id.includes(search) && !name.toLocaleLowerCase().includes(search)) continue;
    const row = document.createElement("label");
    row.className = "group-row";
    const checkbox = document.createElement("input");
    checkbox.type = "checkbox";
    checkbox.value = id;
    checkbox.checked = selected.has(id);
    checkbox.setAttribute("aria-label", "记录 " + name + "（群号 " + id + "）");
    checkbox.addEventListener("change", () => {
      if (checkbox.checked) selected.add(id);
      else selected.delete(id);
      updatePicked();
    });
    const identity = document.createElement("span");
    const title = document.createElement("b");
    title.textContent = name;
    const number = document.createElement("small");
    number.textContent = group.online ? id : id + " · 暂未在线识别";
    identity.append(title, number);
    const count = document.createElement("span");
    count.className = "members";
    count.textContent = group.member_count == null ? "人数待获取" : group.member_count.toLocaleString("zh-CN") + " 人";
    row.append(checkbox, identity, count);
    rows.append(row);
  }
  if (!rows.children.length) {
    const empty = document.createElement("p");
    empty.className = "group-empty";
    empty.textContent = discovering ? "正在读取群列表…" : search ? "没有匹配的群" : "暂无可勾选的 QQ 群";
    rows.append(empty);
  }
  updatePicked();
}

function updatePicked() {
  field("picked").textContent = "已勾选 " + selected.size + " 个群，保存后生效。";
}

function renderExportGroups() {
  const lookup = new Map(directory.map(group => [group.group_id, group.group_name]));
  const dropdown = field("exportGroup"), previous = dropdown.value;
  dropdown.replaceChildren(new Option("全部归档群", ""));
  for (const group of archived) {
    const name = lookup.get(group.group_id);
    const label = (name ? name + " · " : "") + group.group_id + "（" + group.messages + " 条）";
    dropdown.add(new Option(label, group.group_id));
  }
  if (archived.some(group => group.group_id === previous)) dropdown.value = previous;
}

async function discover() {
  if (discovering) return;
  discovering = true;
  field("reloadGroups").disabled = true;
  notice("discovery", "正在识别机器人加入的 QQ 群…");
  renderGroups();
  try {
    const result = await bridge.apiGet("groups");
    const current = new Map(result.groups.map(group => [group.group_id, group]));
    for (const group of directory) {
      if (!current.has(group.group_id) && (selected.has(group.group_id) || saved?.group_ids.includes(group.group_id))) {
        current.set(group.group_id, {...group, online: false});
      }
    }
    directory = Array.from(current.values());
    const live = directory.filter(group => group.online).length;
    const message = !result.accounts ? "尚未连接 OneBot。连接机器人后可刷新群列表。"
      : "已识别 " + live + " 个群" + (result.failed ? "，有 " + result.failed + " 个连接未响应。" : "。");
    notice("discovery", message, Boolean(result.failed));
    renderExportGroups();
  } catch (error) {
    notice("discovery", "群列表获取失败，可重新刷新。" + error.message, true);
  } finally {
    discovering = false;
    field("reloadGroups").disabled = false;
    renderGroups();
  }
}

async function refresh() {
  const state = await bridge.apiGet("state");
  used = state.used_bytes;
  if (!saved || JSON.stringify(saved) !== JSON.stringify(state.settings)) {
    saved = state.settings;
    selected = new Set(saved.group_ids);
    field("capacity").value = saved.capacity_mib;
    field("rolling").checked = saved.rolling;
  }
  field("scope").textContent = state.settings.group_ids.length
    ? "正在采集 " + state.settings.group_ids.length + " 个指定群" : "尚未指定采集群";
  field("used").textContent = size(used) + " / " + size(saved.capacity_mib * mib);
  field("meter").value = Math.min(100, used / (saved.capacity_mib * mib) * 100);
  field("messages").textContent = state.messages.toLocaleString();
  field("media").textContent = state.media.toLocaleString();
  const details = [
    [state.pending, "条正在入库"], [state.missing, "个媒体未取到"],
    [state.evicted, "条累计清理"], [state.skipped, "条因容量跳过"],
    [state.errors, "次本次运行写入失败"]
  ].filter(([n]) => n > 0).map(([n, label]) => n + " " + label);
  field("details").textContent = details.join(" · ");
  field("details").hidden = !details.length;
  archived = state.groups;
  renderExportGroups();
  renderGroups();
  field("settingFields").disabled = false;
  field("exportFields").disabled = false;
  retention();
}

field("capacity").addEventListener("input", retention);
field("rolling").addEventListener("change", retention);
field("groupSearch").addEventListener("input", renderGroups);
field("reloadGroups").addEventListener("click", discover);
field("refresh").addEventListener("click", async () => {
  field("refresh").disabled = true;
  try {
    await refresh();
    notice("pageError");
  } catch (error) { notice("pageError", error.message, true); }
  finally { field("refresh").disabled = false; }
});

field("settings").addEventListener("submit", async event => {
  event.preventDefault();
  const settings = {
    group_ids: Array.from(selected),
    capacity_mib: Number(field("capacity").value), rolling: field("rolling").checked
  };
  field("settingFields").disabled = true;
  field("refresh").disabled = true;
  notice("saveResult", "正在保存…");
  try {
    const result = await bridge.apiPost("settings", {settings, previous: saved});
    saved = result.settings;
    await refresh();
    notice("saveResult", "设置已保存。");
  } catch (error) { notice("saveResult", error.message, true); }
  finally {
    field("settingFields").disabled = false;
    field("refresh").disabled = false;
  }
});

function dayBoundary(value, nextDay) {
  const date = new Date(value + "T00:00:00");
  if (nextDay) date.setDate(date.getDate() + 1);
  return Math.floor(date.getTime() / 1000);
}

async function downloadCorpus(params) {
  let job = await bridge.apiPost("export/start", params);
  const ticket = {id: job.id};
  while (job.phase === "packing") {
    notice("exportResult", "正在打包已入库的记录，已处理 " + job.messages.toLocaleString() +
      " 条消息、" + job.media_files.toLocaleString() + " 个附件，已写入 " + size(job.packed_bytes) + "。");
    await new Promise(resolve => setTimeout(resolve, 750));
    job = await bridge.apiGet("export/status", ticket);
  }
  if (job.phase === "failed") throw new Error("打包失败，请检查服务器的临时磁盘空间及读写权限后重试。");
  if (job.phase !== "ready") throw new Error("此语料包已被领取，请查看浏览器的下载记录。");

  const total = size(job.size);
  notice("exportResult", "ZIP 已生成（" + total + "），正在传给浏览器。接收完成后可保存文件。");
  let downloading = true, timer;
  async function showTransfer() {
    try {
      const state = await bridge.apiGet("export/status", ticket);
      if (!downloading) return;
      if (state.phase === "sent") {
        notice("exportResult", "服务器已发送全部 " + total + "，正在等待浏览器完成接收。");
        return;
      }
      if (state.phase === "sending") {
        notice("exportResult", "ZIP 已生成，服务器已发送 " + size(state.sent) + " / " + total +
          "。请保持此页打开，浏览器接收完成后可保存文件。");
      }
    } catch {
      // A failed progress request does not interrupt the file transfer.
    }
    if (downloading) timer = setTimeout(showTransfer, 1500);
  }
  timer = setTimeout(showTransfer, 1500);
  try {
    return await bridge.download("export", ticket);
  } finally {
    downloading = false;
    clearTimeout(timer);
  }
}

field("export").addEventListener("submit", async event => {
  event.preventDefault();
  const params = {};
  const start = field("since").value, end = field("until").value;
  if (start && end && start > end) {
    notice("exportResult", "结束日期不能早于开始日期。", true);
    return;
  }
  if (start) params.since = dayBoundary(start, false);
  if (end) params.before = dayBoundary(end, true);
  if (field("exportGroup").value) params.group_id = field("exportGroup").value;
  field("download").disabled = true;
  notice("exportResult", "正在创建归档快照…");
  try {
    await downloadCorpus(params);
    notice("exportResult", "语料包已交给浏览器下载。");
  } catch (error) { notice("exportResult", error.message, true); }
  finally { field("download").disabled = false; }
});

async function start() {
  try {
    if (!bridge) throw new Error("请在 AstrBot 管理后台打开语料归档页。");
    const theme = ctx => { document.documentElement.dataset.dark = String(Boolean(ctx?.isDark)); };
    theme(await bridge.ready());
    bridge.onContext?.(theme);
    field("refresh").disabled = false;
    await Promise.all([refresh(), discover()]);
  } catch (error) { notice("pageError", error.message, true); }
}
start();
