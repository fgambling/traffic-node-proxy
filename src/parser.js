/**
 * Parse a 26-dimensional sigmoid probability array into structured traffic_fact columns.
 *
 * Attribute index reference (from vendor doc):
 *  0  Hat          1  Glasses     2  ShortSleeve   3  LongSleeve
 *  4  UpperStride  5  UpperLogo   6  UpperPlaid    7  UpperSplice
 *  8  LowerStripe  9  LowerPattern 10 LongCoat     11 Trousers
 * 12  Shorts      13  Skirt&Dress 14 Boots         15 HandBag
 * 16  ShoulderBag 17  Backpack    18 HoldObjectsInFront
 * 19  AgeLess18   20  Age18-60    21 AgeOver60     22 Female
 * 23  Front       24  Side        25 Back
 */
const THRESHOLD = 0.5;

function argmax(arr) {
  let best = 0;
  for (let i = 1; i < arr.length; i++) if (arr[i] > arr[best]) best = i;
  return best;
}

function parseAttributes(attrs) {
  if (!Array.isArray(attrs) || attrs.length !== 26) return null;

  // ── Gender (exclusive, index 22) ──────────────────────────────
  const isFemale = attrs[22] > THRESHOLD;

  // ── Age (exclusive, indices 19-21) ────────────────────────────
  const ageIdx = argmax([attrs[19], attrs[20], attrs[21]]);

  // ── Upper clothing (exclusive, indices 2, 3, 10) ──────────────
  const upperIdx = argmax([attrs[2], attrs[3], attrs[10]]);

  // ── Lower clothing (exclusive, indices 11, 12, 13) ────────────
  const lowerIdx = argmax([attrs[11], attrs[12], attrs[13]]);

  return {
    // gender
    gender_male:         isFemale ? 0 : 1,
    gender_female:       isFemale ? 1 : 0,
    // age
    age_under18:         ageIdx === 0 ? 1 : 0,
    age_18_60:           ageIdx === 1 ? 1 : 0,
    age_over60:          ageIdx === 2 ? 1 : 0,
    // accessories (multi-select)
    accessory_hat:       attrs[0]  > THRESHOLD ? 1 : 0,
    accessory_glasses:   attrs[1]  > THRESHOLD ? 1 : 0,
    accessory_boots:     attrs[14] > THRESHOLD ? 1 : 0,
    bag_handbag:         attrs[15] > THRESHOLD ? 1 : 0,
    bag_shoulder:        attrs[16] > THRESHOLD ? 1 : 0,
    bag_backpack:        attrs[17] > THRESHOLD ? 1 : 0,
    hold_item:           attrs[18] > THRESHOLD ? 1 : 0,
    // upper clothing type (exclusive)
    upper_short:         upperIdx === 0 ? 1 : 0,
    upper_long:          upperIdx === 1 ? 1 : 0,
    upper_coat:          upperIdx === 2 ? 1 : 0,
    // upper style (multi-select)
    upper_style_stripe:  attrs[4]  > THRESHOLD ? 1 : 0,
    upper_style_logo:    attrs[5]  > THRESHOLD ? 1 : 0,
    upper_style_plaid:   attrs[6]  > THRESHOLD ? 1 : 0,
    upper_style_splice:  attrs[7]  > THRESHOLD ? 1 : 0,
    // lower clothing type (exclusive)
    lower_trousers:      lowerIdx === 0 ? 1 : 0,
    lower_shorts:        lowerIdx === 1 ? 1 : 0,
    lower_skirt:         lowerIdx === 2 ? 1 : 0,
    // lower style (multi-select)
    lower_style_stripe:  attrs[8]  > THRESHOLD ? 1 : 0,
    lower_style_pattern: attrs[9]  > THRESHOLD ? 1 : 0,
  };
}

/**
 * Floor a Date to minute precision (seconds = 0, ms = 0).
 * This matches traffic_fact.time_bucket convention.
 */
function toTimeBucket(dateInput) {
  const d = new Date(dateInput);
  d.setSeconds(0, 0);
  // Format as 'YYYY-MM-DD HH:MM:00' in Asia/Shanghai (+08:00)
  const offset = 8 * 60;
  const local = new Date(d.getTime() + offset * 60000);
  return local.toISOString().replace('T', ' ').substring(0, 19);
}

module.exports = { parseAttributes, toTimeBucket };
