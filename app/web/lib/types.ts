export type LayerType = "kebenaran_merancang" | "pelan_bangunan" | "kerja_tanah";

export type Filters = {
  layerTypes: LayerType[];
  statuses: string[];
  years: number[];
  planningBlocks: string[];
  mukims: string[];
};

export type FilterOptions = {
  statuses: string[];
  years: number[];
  planningBlocks: string[];
  mukims: string[];
};

export type OverviewStats = {
  totalFeatures: number;
  approvedFeatures: number;
  pendingFeatures: number;
  totalAreaAcres: number;
};

export type DistributionRow = {
  key: string | number | null;
  label: string;
  count: number;
};

export type SearchResult = {
  applicationId: string;
  referenceNo: string | null;
  title: string;
  layerType: string;
  status: string;
  planningBlock: string | null;
  mukim: string | null;
  centroidLon: number;
  centroidLat: number;
};

export type ApplicationDetail = {
  kind: "application";
  applicationId: string;
  referenceNo: string | null;
  referenceNoAlt: string | null;
  title: string;
  status: string;
  layerType: string;
  applicationType: string;
  applicationYear: number | null;
  approvalYear: number | null;
  lotNo: string | null;
  mukim: string | null;
  planningBlock: string | null;
  zoningName: string | null;
  developerName: string | null;
  consultantName: string | null;
  areaAcres: number;
  areaM2: number;
  centroidLon: number;
  centroidLat: number;
};

export type PlanningBlockPoint = {
  planningBlockId: string;
  planningBlock: string;
  zoningName: string | null;
  mukim: string | null;
  centroidLon: number;
  centroidLat: number;
  featureCount: number;
};

export type PlanningBlockDetail = {
  kind: "planning_block";
  planningBlockId: string;
  planningBlock: string;
  zoningName: string | null;
  mukim: string | null;
  areaAcres: number;
  areaM2: number;
  centroidLon: number;
  centroidLat: number;
  featureCount: number;
  approvedCount: number;
  pendingCount: number;
  totalDevelopmentAreaAcres: number;
  layerBreakdown: Array<{
    layerType: string;
    count: number;
  }>;
};

export type FeatureDetail = ApplicationDetail | PlanningBlockDetail;

export type HoverState = {
  x: number;
  y: number;
  applicationId: string;
  referenceNo: string | null;
  title: string;
  layerType: string;
  status: string;
};
