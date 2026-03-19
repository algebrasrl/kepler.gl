export const INVALID_BASELINE_REASONS = Object.freeze([
  'dry_run_report',
  'transport_aborted',
  'empty_cases',
  'all_cases_transport_skipped',
  'zero_metric_aborted_window'
]);

export function isAllowedInvalidBaselineReason(value) {
  return INVALID_BASELINE_REASONS.includes(String(value || '').trim());
}

export function validateEvalReportContract(report, reportLabel = 'report') {
  const errors = [];
  if (!report || typeof report !== 'object' || Array.isArray(report)) {
    errors.push(`${reportLabel} must be an object`);
    return errors;
  }

  if ('invalidBaseline' in report && typeof report.invalidBaseline !== 'boolean') {
    errors.push(`${reportLabel}.invalidBaseline must be a boolean when present`);
  }
  if ('invalidBaselineReason' in report) {
    const reason = report.invalidBaselineReason;
    if (reason !== null && reason !== undefined && typeof reason !== 'string') {
      errors.push(`${reportLabel}.invalidBaselineReason must be null or a string`);
    }
  }

  if (report.invalidBaseline === true) {
    const reason = String(report.invalidBaselineReason || '').trim();
    if (!reason) {
      errors.push(`${reportLabel}.invalidBaselineReason must be set when invalidBaseline=true`);
    } else if (!isAllowedInvalidBaselineReason(reason)) {
      errors.push(
        `${reportLabel}.invalidBaselineReason must be one of: ${INVALID_BASELINE_REASONS.join(', ')}`
      );
    }
    if (String(report.runType || '').trim().toLowerCase() !== 'baseline') {
      errors.push(`${reportLabel}.invalidBaseline=true is only valid for baseline reports`);
    }
  } else if (report.invalidBaselineReason !== null && report.invalidBaselineReason !== undefined) {
    const reason = String(report.invalidBaselineReason || '').trim();
    if (reason) {
      errors.push(
        `${reportLabel}.invalidBaselineReason must be null/empty unless invalidBaseline=true`
      );
    }
  }

  return errors;
}
