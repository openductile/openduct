// Copyright 2026 Ductile AI, Inc
// SPDX-License-Identifier: Apache-2.0

// The workflow as a diagram: what starts it, its steps joined by arrows, branches side by side, loops boxed. Click
// a step to change it and see what it gave back; the + on an arrow adds a step there. After a test or a preview
// each step shows whether it worked and how long it took.

import {
  Background,
  BackgroundVariant,
  BaseEdge,
  Controls,
  type Edge,
  EdgeLabelRenderer,
  type EdgeProps,
  getSmoothStepPath,
  Handle,
  MarkerType,
  type Node,
  type NodeProps,
  Position,
  ReactFlow,
  type ReactFlowInstance,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";
import { useEffect, useMemo, useRef } from "react";
import type { Definition, Run, Step } from "../api";
import { KINDS, kindOf } from "../Steps";
import { Icon } from "./Icon";
import { layout, type LaidNode } from "./layout";
import { FAMILY, summaryOf, triggerSummary } from "./look";
import { partsOf, statusOf, type Status } from "./Output";
import { sameStep, type StepPath } from "./paths";

export type Selection = { kind: "trigger" } | { kind: "step"; at: StepPath } | null;

type StepData = {
  step: Step;
  at: StepPath;
  status?: Status;
  ms?: number;
  selected: boolean;
};

type TriggerData = { trigger: unknown; selected: boolean };
type EmptyData = { at: StepPath; label?: string; onAdd: (at: StepPath) => void };
type EdgeData = { insert?: StepPath; label?: string; onAdd: (at: StepPath) => void };

const hidden = { opacity: 0, width: 6, height: 6, minWidth: 0, minHeight: 0, border: "none" };

function Ins() {
  return <Handle type="target" position={Position.Top} style={hidden} isConnectable={false} />;
}
function Outs() {
  return <Handle type="source" position={Position.Bottom} style={hidden} isConnectable={false} />;
}

function StatusMark({ status, ms }: { status?: Status; ms?: number }) {
  if (!status || status === "unreached") return null;
  const mark = status === "done" ? "✓" : status === "failed" ? "✕" : "–";
  return (
    <span className={`mark mark-${status}`} title={status === "stopped" ? "the run ended here" : status}>
      {mark}
      {ms !== undefined && status !== "stopped" && <small>{ms} ms</small>}
    </span>
  );
}

function StepNode({ data }: NodeProps<Node<StepData>>) {
  const kind = kindOf(data.step);
  const label = KINDS.find((k) => k.kind === kind)?.label ?? kind;
  const classes = ["flow-node", `family-${FAMILY[kind] ?? "data"}`];
  if (data.selected) classes.push("selected");
  if (data.status) classes.push(`status-${data.status}`);
  return (
    <div className={classes.join(" ")} title={label}>
      <Ins />
      <span className="icon">
        <Icon kind={kind} />
      </span>
      <span className="words">
        <strong>{data.step.name ?? label}</strong>
        <span>{summaryOf(kind, data.step) || label}</span>
      </span>
      <StatusMark status={data.status} ms={data.ms} />
      <Outs />
      {kind === "for_each" && <Handle id="back" type="target" position={Position.Right} style={hidden} isConnectable={false} />}
    </div>
  );
}

function TriggerNode({ data }: NodeProps<Node<TriggerData>>) {
  const { kind, line } = triggerSummary(data.trigger);
  return (
    <div className={`flow-node family-start trigger${data.selected ? " selected" : ""}`}>
      <span className="icon">
        <Icon kind={kind} />
      </span>
      <span className="words">
        <small>Starts when</small>
        <strong>{line}</strong>
      </span>
      <Outs />
    </div>
  );
}

function JoinNode() {
  return (
    <div className="flow-join">
      <Ins />
      <Outs />
      <Handle id="back" type="source" position={Position.Right} style={hidden} isConnectable={false} />
    </div>
  );
}

function EmptyNode({ data }: NodeProps<Node<EmptyData>>) {
  return (
    <button className="flow-empty nodrag" onClick={() => data.onAdd(data.at)}>
      <Ins />
      + add a step
      <Outs />
    </button>
  );
}

function EndNode() {
  return (
    <div className="flow-end">
      <Ins />
      End
    </div>
  );
}

function LoopBox() {
  return (
    <div className="flow-box">
      <span>for each</span>
    </div>
  );
}

function AddEdge(props: EdgeProps<Edge<EdgeData>>) {
  const [path, labelX, labelY] = getSmoothStepPath({ ...props, borderRadius: 12 });
  const insert = props.data?.insert;
  return (
    <>
      <BaseEdge id={props.id} path={path} markerEnd={props.markerEnd} />
      <EdgeLabelRenderer>
        <div className="edge-tools nodrag nopan" style={{ transform: `translate(-50%, -50%) translate(${labelX}px, ${labelY}px)` }}>
          {props.data?.label && <span className="edge-label">{props.data.label}</span>}
          {insert && (
            <button className="edge-add" aria-label="Add a step here" title="Add a step here" onClick={() => props.data?.onAdd(insert)}>
              +
            </button>
          )}
        </div>
      </EdgeLabelRenderer>
    </>
  );
}

function BackEdge(props: EdgeProps<Edge<EdgeData>>) {
  const [path, labelX, labelY] = getSmoothStepPath({ ...props, borderRadius: 12, offset: 36 });
  return (
    <>
      <BaseEdge id={props.id} path={path} markerEnd={props.markerEnd} className="back-edge" />
      <EdgeLabelRenderer>
        <span className="edge-label back" style={{ transform: `translate(-50%, -50%) translate(${labelX + 18}px, ${labelY}px)` }}>
          next item
        </span>
      </EdgeLabelRenderer>
    </>
  );
}

const nodeTypes = { step: StepNode, trigger: TriggerNode, join: JoinNode, empty: EmptyNode, end: EndNode, "loop-box": LoopBox };
const edgeTypes = { add: AddEdge, back: BackEdge };

export function Canvas(props: {
  definition: Definition;
  run: Run | null;
  selection: Selection;
  onSelect: (s: Selection) => void;
  onAdd: (at: StepPath) => void;
}) {
  const laid = useMemo(() => layout(props.definition), [props.definition]);
  const parts = useMemo(() => partsOf(props.run), [props.run]);
  const nodes: Node[] = laid.nodes.map((n: LaidNode) => {
    const base = { id: n.id, position: { x: n.x, y: n.y }, draggable: false, connectable: false };
    switch (n.kind) {
      case "step": {
        const name = n.step?.name;
        const data: StepData = {
          step: n.step!,
          at: n.at!,
          status: statusOf(props.run, name),
          ms: name ? parts.get(name)?.ms : undefined,
          selected: props.selection?.kind === "step" && sameStep(props.selection.at, n.at!),
        };
        return { ...base, type: "step", data, style: { width: n.width } };
      }
      case "trigger":
        return { ...base, type: "trigger", data: { trigger: props.definition.trigger, selected: props.selection?.kind === "trigger" }, style: { width: n.width } };
      case "empty":
        return { ...base, type: "empty", data: { at: n.at!, label: n.label, onAdd: props.onAdd }, style: { width: n.width } };
      case "loop-box":
        return { ...base, type: "loop-box", data: {}, selectable: false, zIndex: -1, style: { width: n.width, height: n.height } };
      case "end":
        return { ...base, type: "end", data: {}, selectable: false, style: { width: n.width } };
      default:
        return { ...base, type: "join", data: {}, selectable: false };
    }
  });
  const edges: Edge[] = laid.edges.map((e) => ({
    id: e.id,
    source: e.source,
    target: e.target,
    type: e.back ? "back" : "add",
    sourceHandle: e.back ? "back" : undefined,
    targetHandle: e.back ? "back" : undefined,
    data: { insert: e.insert, label: e.label, onAdd: props.onAdd },
    markerEnd: { type: MarkerType.ArrowClosed, width: 12, height: 12 },
  }));
  // When the canvas changes size - a window made smaller, the side folded, its tab shown after it was opened hidden -
  // the whole workflow is fitted again. Back at the size it was last seen at (its tab shown again), the view is
  // left as it was, zoom and all.
  const flow = useRef<ReactFlowInstance | null>(null);
  const box = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = box.current;
    if (!el || typeof ResizeObserver === "undefined") return;
    let last = "";
    const seen = new ResizeObserver(([entry]) => {
      if (!entry) return;
      const w = Math.round(entry.contentRect.width);
      const h = Math.round(entry.contentRect.height);
      // Hidden: nothing to fit, and the size it had is kept to compare with.
      if (w === 0 || h === 0) return;
      const size = `${w}x${h}`;
      if (size === last) return;
      last = size;
      requestAnimationFrame(() => flow.current?.fitView({ padding: 0.18, maxZoom: 1 }));
    });
    seen.observe(el);
    return () => seen.disconnect();
  }, []);
  return (
    <div className="canvas" ref={box}>
      <ReactFlow
        onInit={(instance) => {
          flow.current = instance;
        }}
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        edgeTypes={edgeTypes}
        fitView
        fitViewOptions={{ padding: 0.18, maxZoom: 1 }}
        minZoom={0.2}
        maxZoom={1.6}
        nodesDraggable={false}
        nodesConnectable={false}
        onNodeClick={(_, node) => {
          if (node.type === "trigger") props.onSelect({ kind: "trigger" });
          else if (node.type === "step") props.onSelect({ kind: "step", at: (node.data as StepData).at });
        }}
        onPaneClick={() => props.onSelect(null)}
      >
        <Background variant={BackgroundVariant.Dots} gap={22} size={1.2} />
        <Controls showInteractive={false} />
      </ReactFlow>
    </div>
  );
}
