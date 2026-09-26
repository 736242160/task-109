#!/usr/bin/env python3
"""group_allocator.py — 分组归属约束校验工具（纯标准库，单文件）

输入（JSON，从文件参数或 stdin 读取）：
{
  "groups": [
    {"name": "A", "capacity": 100, "mutex": ["B"], "share": "pool1"}
    // name: 组名(必填, 唯一); capacity: 容量上限(必填, 非负数)
    // mutex: 互斥组列表(可选); share: 共享容量池名(可选, 同池组共享一个上限)
  ],
  "items": [   // 数据项事件流，按顺序处理
    {"op": "upsert", "name": "x", "groups": ["A"], "value": 10},
    {"op": "remove", "name": "x"}
    // upsert: 新增或整体替换该项（同名为更新，支持跨组迁移与值变化）
    // remove: 删除该项
  ]
}

规则说明：
- 重复归属：允许一项同属多个非互斥组，其值全额计入每个组的容量
  （保守记账：每个组都必须能独立容纳该项）。
- 互斥：对称关系；一项不得同时属于互斥的两个组，违反即报告。
- 共享容量：share 相同的组共享一个上限，池上限取成员 capacity 的最小值。
- 引用校验：项引用不存在的组、组引用不存在的互斥组，均报告。

输出（JSON 到 stdout）：{"groups": {...各组状态...}, "errors": [...]}
退出码：0 无错误；1 有校验错误；2 输入本身无法解析。
用法：python3 group_allocator.py [input.json]   或   python3 group_allocator.py --demo
"""

import json
import sys


def err(errors, etype, message, **details):
    e = {"type": etype, "message": message}
    e.update(details)
    errors.append(e)


def parse_groups(raw_groups, errors):
    groups = {}
    for i, g in enumerate(raw_groups):
        if not isinstance(g, dict) or not isinstance(g.get("name"), str):
            err(errors, "INVALID_GROUP_DEF",
                "groups[%d] 缺少合法 name，已跳过" % i, index=i)
            continue
        name = g["name"]
        if name in groups:
            err(errors, "DUPLICATE_GROUP",
                "组 %r 重复定义，保留第一份" % name, group=name)
            continue
        cap = g.get("capacity")
        if not isinstance(cap, (int, float)) or isinstance(cap, bool) or cap < 0:
            err(errors, "INVALID_CAPACITY",
                "组 %r 的 capacity 必须是非负数值，已按 0 处理" % name, group=name)
            cap = 0
        mutex = g.get("mutex", [])
        if not isinstance(mutex, list):
            err(errors, "INVALID_MUTEX",
                "组 %r 的 mutex 必须是列表，已按空处理" % name, group=name)
            mutex = []
        share = g.get("share")
        if share is not None and not isinstance(share, str):
            err(errors, "INVALID_SHARE",
                "组 %r 的 share 必须是字符串，已忽略" % name, group=name)
            share = None
        groups[name] = {"capacity": cap, "mutex": list(mutex), "share": share}

    # 互斥引用校验 + 对称化
    for name, g in groups.items():
        for m in g["mutex"]:
            if m not in groups:
                err(errors, "UNKNOWN_MUTEX_GROUP",
                    "组 %r 引用了不存在的互斥组 %r" % (name, m),
                    group=name, mutex_group=m)
            elif name not in groups[m]["mutex"]:
                groups[m]["mutex"].append(name)  # 互斥按对称处理
    return groups


def process_items(raw_items, groups, errors):
    items = {}  # name -> {"groups": [...], "value": float}
    for i, ev in enumerate(raw_items):
        if not isinstance(ev, dict):
            err(errors, "INVALID_EVENT", "items[%d] 不是对象，已跳过" % i, index=i)
            continue
        op = ev.get("op", "upsert")
        name = ev.get("name")
        if not isinstance(name, str):
            err(errors, "INVALID_EVENT",
                "items[%d] 缺少合法 name，已跳过" % i, index=i)
            continue
        if op == "remove":
            if name not in items:
                err(errors, "ITEM_NOT_FOUND",
                    "remove：数据项 %r 不存在" % name, item=name)
            else:
                del items[name]
            continue
        if op != "upsert":
            err(errors, "INVALID_EVENT",
                "items[%d] 未知 op %r，已跳过" % (i, op), index=i)
            continue
        value = ev.get("value")
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            err(errors, "INVALID_VALUE",
                "数据项 %r 的 value 必须是数值，事件已跳过" % name, item=name)
            continue
        raw_gs = ev.get("groups", [])
        if not isinstance(raw_gs, list):
            err(errors, "INVALID_EVENT",
                "数据项 %r 的 groups 必须是列表，事件已跳过" % name, item=name)
            continue
        valid, seen = [], set()
        for gn in raw_gs:
            if gn not in groups:
                err(errors, "UNKNOWN_GROUP",
                    "数据项 %r 引用了不存在的组 %r" % (name, gn),
                    item=name, group=gn)
            elif gn not in seen:
                seen.add(gn)
                valid.append(gn)
        if not valid:
            err(errors, "ITEM_DROPPED",
                "数据项 %r 没有任何有效归属组，未入库" % name, item=name)
            items.pop(name, None)
            continue
        # upsert 整体替换：旧归属自动失效，实现跨组迁移与值变化重判
        items[name] = {"groups": valid, "value": value}
    return items


def build_report(groups, items, errors):
    # 归属统计
    used = {n: 0 for n in groups}
    members = {n: [] for n in groups}
    for iname, it in items.items():
        for gn in it["groups"]:
            used[gn] += it["value"]
            members[gn].append(iname)

    # 互斥校验：一项不得同时属于互斥的两组
    for iname, it in items.items():
        gs = it["groups"]
        for a in range(len(gs)):
            for b in range(a + 1, len(gs)):
                if gs[b] in groups[gs[a]]["mutex"]:
                    err(errors, "MUTEX_VIOLATION",
                        "数据项 %r 同时属于互斥组 %r 和 %r" % (iname, gs[a], gs[b]),
                        item=iname, groups=[gs[a], gs[b]])

    # 共享池：limit 取成员 capacity 最小值
    pools = {}
    for n, g in groups.items():
        if g["share"]:
            pools.setdefault(g["share"], []).append(n)
    pool_info = {}
    for pname, gnames in pools.items():
        limit = min(groups[n]["capacity"] for n in gnames)
        pused = sum(used[n] for n in gnames)
        pool_info[pname] = {"groups": sorted(gnames), "limit": limit, "used": pused}
        if pused > limit:
            err(errors, "CAPACITY_EXCEEDED",
                "共享池 %r（组 %s）超出容量：上限 %s，已用 %s，超出 %s"
                % (pname, ", ".join(sorted(gnames)), limit, pused, pused - limit),
                pool=pname, groups=sorted(gnames), limit=limit,
                used=pused, excess=pused - limit)

    # 独立组容量校验
    for n, g in groups.items():
        if g["share"]:
            continue  # 共享组由池统一校验
        if used[n] > g["capacity"]:
            err(errors, "CAPACITY_EXCEEDED",
                "组 %r 超出容量：上限 %s，已用 %s，超出 %s"
                % (n, g["capacity"], used[n], used[n] - g["capacity"]),
                group=n, limit=g["capacity"], used=used[n],
                excess=used[n] - g["capacity"])

    # 输出分组状态
    status = {}
    for n in sorted(groups):
        g = groups[n]
        s = {"capacity": g["capacity"], "used": used[n],
             "remaining": g["capacity"] - used[n],
             "items": sorted(members[n]),
             "mutex": sorted(g["mutex"])}
        if g["share"]:
            p = pool_info[g["share"]]
            s["shared_pool"] = {"name": g["share"], "limit": p["limit"],
                                "used": p["used"],
                                "remaining": p["limit"] - p["used"]}
        status[n] = s
    return {"groups": status, "errors": errors}


DEMO_INPUT = {
    "groups": [
        {"name": "fast",  "capacity": 100, "mutex": ["slow"]},
        {"name": "slow",  "capacity": 50},
        {"name": "cache", "capacity": 80, "share": "mem"},
        {"name": "index", "capacity": 80, "share": "mem"},
        {"name": "ghost", "capacity": 10, "mutex": ["nowhere"]},
    ],
    "items": [
        {"op": "upsert", "name": "a", "groups": ["fast"], "value": 30},
        {"op": "upsert", "name": "b", "groups": ["cache", "index"], "value": 50},
        {"op": "upsert", "name": "c", "groups": ["cache"], "value": 40},
        {"op": "upsert", "name": "a", "groups": ["slow"], "value": 20},  # 迁移 fast->slow
        {"op": "upsert", "name": "d", "groups": ["fast", "slow"], "value": 5},   # 互斥冲突
        {"op": "upsert", "name": "e", "groups": ["fast", "void"], "value": 200}, # 未知组+超容
        {"op": "remove", "name": "c"},
        {"op": "remove", "name": "nobody"},
    ],
}


def main(argv):
    if len(argv) > 1 and argv[1] == "--demo":
        data = DEMO_INPUT
    else:
        try:
            text = open(argv[1]).read() if len(argv) > 1 else sys.stdin.read()
            data = json.loads(text)
        except (OSError, json.JSONDecodeError, IndexError) as e:
            print("输入解析失败: %s" % e, file=sys.stderr)
            return 2
    errors = []
    groups = parse_groups(data.get("groups", []), errors)
    items = process_items(data.get("items", []), groups, errors)
    report = build_report(groups, items, errors)
    json.dump(report, sys.stdout, ensure_ascii=False, indent=2)
    print()
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
