import {z} from 'zod';

/** Exact dataset name or id reference (e.g. from listQMapDatasets). */
export const DATASET_NAME_PARAM = z.string().trim().min(1);

/** Optional layer name or id to target. */
export const OPTIONAL_LAYER_NAME_PARAM = z.string().optional();

/** Field name within a dataset. */
export const FIELD_NAME_PARAM = z.string().trim().min(1);

/** Whether to show the derived dataset as a visible map layer. */
export const SHOW_ON_MAP_PARAM = z.boolean().optional();

/** Optional custom name for the output dataset. */
export const NEW_DATASET_NAME_PARAM = z.string().optional();
