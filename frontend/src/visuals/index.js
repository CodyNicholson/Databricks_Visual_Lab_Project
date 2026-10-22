import { ConnectVisual, LazyVisual, ShuffleVisual, SqlVisual, UdfVisual } from './SparkVisuals.jsx';
import {
  CdfVisual, ConstraintsVisual, DeltaLogVisual, MergeVisual, OptimizeVisual, SchemaVisual, TimeTravelVisual, VacuumVisual,
} from './DeltaVisuals.jsx';
import { FunctionsVisual, ManagedExternalVisual, NamespaceVisual, VolumesVisual } from './GovernanceVisuals.jsx';
import { DeltaSourceVisual, FileStreamVisual, MedallionVisual } from './StreamingVisuals.jsx';
import { CdcVisual, PipelineVisual } from './PipelineVisuals.jsx';
import { RegistryVisual, TrackingVisual } from './MlVisuals.jsx';
import { JobsVisual } from './PlatformVisuals.jsx';
import { CollectVisual, RowByRowVisual, SkewVisual } from './MistakeVisuals.jsx';

// Maps each demo's `visual` name (see demos/catalog.js) to the component that draws its data
export const VISUALS = {
  connect: ConnectVisual,
  lazy: LazyVisual,
  sql: SqlVisual,
  shuffle: ShuffleVisual,
  udfs: UdfVisual,
  deltaLog: DeltaLogVisual,
  timeTravel: TimeTravelVisual,
  merge: MergeVisual,
  schema: SchemaVisual,
  optimize: OptimizeVisual,
  vacuum: VacuumVisual,
  cdf: CdfVisual,
  constraints: ConstraintsVisual,
  namespace: NamespaceVisual,
  managedExternal: ManagedExternalVisual,
  volumes: VolumesVisual,
  functions: FunctionsVisual,
  fileStream: FileStreamVisual,
  deltaSource: DeltaSourceVisual,
  medallion: MedallionVisual,
  pipeline: PipelineVisual,
  cdc: CdcVisual,
  tracking: TrackingVisual,
  registry: RegistryVisual,
  jobs: JobsVisual,
  skew: SkewVisual,
  rowByRow: RowByRowVisual,
  collect: CollectVisual,
};
