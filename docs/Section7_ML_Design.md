# 7. Conversion Model and MLOps

This section adds a machine-learning model to the warehouse of Sections 5 and 6, for Problem 2 (high
inquiry, low conversion). The code is in `ml/`, the feature store and registry in
`sql/ml_feature_store.sql`, and `AisleCo_ML_Conversion_Demo.ipynb` runs every step shown here.

## 7.1 The business question

About 14% of inquiries become a booking, and three in four lost inquiries end as "No reply": the
couple stops writing and the 30-day rule closes the inquiry. Sales has a few people for dozens of open
inquiries at any time. The model answers one question each morning: **which open inquiries are most
likely to book?** Sales calls the High band first and follows up the quiet couples before they drop out.
The no-reply rate is tracked as a KPI on the dashboard; the model is how Sales acts on it.

One model, one label: *did this inquiry turn into a booking?* An inquiry is scored every day while it is
open, so its score changes as the couple writes, gets a consultation and a quote.

## 7.2 Data

The model reads only the warehouse: the Silver zone (`stg`), which keeps every CRM row with its
timestamps, and the dimensions. No new source and no schema change in the CRM were needed. The CRM
already records what predicts a booking in practice: the channel, budget and package, the wedding date
and whether it is a peak or auspicious date, our reply time, and the couple's own messages.

The data is simulated (`AisleCo_MultiZone_Architecture_v3.ipynb`, three years, 2024–2026, 1,905
inquiries). Each couple has a hidden readiness to book that drives what the CRM records; the
assumptions are listed in that notebook. A first version of the simulation decided bookings almost at
random, and no model could rank couples (AUC 0.56, the same as the simulation's own probabilities). The
simulation was rebuilt so that behaviour drives outcomes, as it does in real sales. The accuracy below is
therefore the accuracy of the simulation's assumptions; with the real CRM, the same pipeline learns the
real drivers.

## 7.3 Feature store

The feature store is three objects in PostgreSQL, following the entity / time index / features design
of Chapter 7. No separate feature-store product is used.

| Object | Role |
|---|---|
| `feature.inquiry_features(ids, cutoffs)` | The one definition of the features, in SQL. For each inquiry and cutoff time it uses only rows dated before the cutoff, and returns only inquiries still open then |
| `feature."InquiryFeatures"` | Offline store: the features of every open inquiry on every scoring day (key InquiryID + AsOf) |
| `feature."InquiryLabel"` | The outcome of closed inquiries (booked or not) and when it became known |

| Feature | From |
|---|---|
| Channel, budget range, package type (or none) | Inquiry, MarketingChannel, Package |
| Wedding date given, lead days, peak or auspicious date | Inquiry, AuspiciousDate |
| Our first reply time (missing = not logged or in person) | ChatMessage |
| Couple messages so far and in the first 24 h; hours since the couple last wrote | ChatMessage |
| Consulted, quoted, quote share of budget (by the cutoff) | Inquiry |
| Age of the inquiry in days | Inquiry |

**Point in time.** Training calls the function with past cutoffs and the daily scoring calls it with
today, so both get the same features. A training example is an inquiry as it looked 1, 7, 14 and 30 days
after it arrived (while still open), joined with its later outcome. A test checks that adding rows dated
after a cutoff does not change the features at that cutoff. Limit: descriptive fields (channel, budget,
package, wedding date) are read as they are now, because the CRM keeps no history for them; event times
are exact. The offline store keeps each day's features from the first day in production.

## 7.4 Training, evaluation and validation

| Step | Design |
|---|---|
| Split | By time: test = inquiries received 270 to 90 days before the as-of date (old enough to have an outcome), train = everything before |
| Candidates | Logistic regression and gradient boosting (scikit-learn), the same preprocessing: one-hot categories, median imputation with a "was missing" flag |
| Baseline | Each channel's past booking rate |
| Metrics | AUC overall and by inquiry age, PR-AUC, share of bookings in the top 20% of scores, Brier score |
| Gate | Promote only if AUC ≥ baseline + 0.05, and not more than 0.01 below the production model on test inquiries that model never saw |
| Production model | The winner refit on train + test; bands: High = top 20% of scores, Medium = next 30%, Low = the rest |

Results (test window 21 Apr – 16 Oct 2026, model trained on data to 15 Jan 2027):

| | AUC | Day 1 | Day 7 | Day 14 | Day 30 | Top 20% catches |
|---|---|---|---|---|---|---|
| Baseline (channel rate) | 0.68 | | | | | |
| Gradient boosting (production) | **0.87** | 0.76 | 0.86 | 0.90 | 0.96 | 63% of bookings |

The first model, trained on data to 1 Oct 2025, scored 0.80 against a baseline of 0.56. On the first day
the model knows little beyond the channel, budget, package, wedding date, our reply time and the first
messages; by day 7–14 the couple's replies and silences show. Over 15 months of weekly scoring, couples in
the High band booked 39% of the time, Medium 13% and Low 1%.

## 7.5 Serving, monitoring and retraining

| Part | Design |
|---|---|
| Model registry and metadata | `ml."ModelVersion"`: every training run with its data windows, row counts, metrics of all candidates and the baseline, the gate's decision, the training distributions and the model itself. Status production / retired / rejected |
| Serving | Batch: each morning the features of every open inquiry are stored and scored into `ml."InquiryScore"`; the dashboard reads `mart.v_followup_list` |
| Data drift | PSI of channel, budget and package, and of deciles of lead days, reply time and first-day messages, for the inquiries of the last 120 days (on day 1) against the training data. < 0.1 ok, 0.1–0.2 warning, ≥ 0.2 alert. 120 days is longer than the longest gap between wedding fairs (99 days), so the fair calendar does not look like drift |
| Missing values | A feature's missing rate compared with training (≥ 0.05 change warns, ≥ 0.15 alerts) |
| Performance | Live AUC of the scores given 30–180 days ago whose outcome is now known; 0.05 below the test AUC warns, 0.1 alerts |
| Alerts | `ml."DriftCheck"`, and alerts in `etl."Alert"` next to the ETL alerts |
| Retraining | Weekly (`aisleco_weekly_train`), and by hand after a drift alert. A retrained model replaces production only through the gate |

In the simulation, a TikTok campaign in 2026 made TikTok about 19% of inquiries instead of 6%, and those
couples book far less often. The channel PSI was at most 0.12 in 2024–2025 and rose to 0.2–0.4 in 2026;
the first alert came on 16 Feb 2026. Retraining followed, and the registry shows each version and the one
the gate rejected. The live AUC stayed between about 0.85 and 0.9: the model already knew that TikTok
couples book less, so drift raised a flag without making the model worse.

## 7.6 Automation and the dashboard

| Airflow DAG | Tasks |
|---|---|
| `aisleco_daily_etl` (02:00) | The 11 ETL steps, then `ml_features → ml_score → ml_monitor` |
| `aisleco_weekly_train` (Sunday 04:00) | `ml train`: training set, candidates, gate, registry |

The same steps run from the command line (`python -m ml features|score|monitor|daily|train|status`).

The dashboard (`dashboard/app.py`, Streamlit) reads only `mart` views: Problem 1 capacity (weekly
utilization heatmap, peak-date acceptance, couples turned away), Problem 2 conversion (funnel by channel,
no-reply rate by month, reply speed, lost reasons), the follow-up list and model health.

## 7.7 MLOps level

| Level (Chapter 7) | This project |
|---|---|
| 0: manual | Passed: no manual step between data and prediction |
| 1: ML pipeline automation | Yes: automated training pipeline with continuous training, feature store, metadata store and registry, validation gate, monitoring with alerts |
| 2: CI/CD of the pipeline | Partly: 38 automated tests (5 for the model) run with one command, but not yet on every push. The next step is a GitHub Actions workflow running `pytest` against a PostgreSQL service |

## 7.8 Tests

| Test | Result |
|---|---|
| Features at a past cutoff do not change when later rows (messages, consultations, quotes) are added | Pass |
| Training rows are dated before the inquiry closed; the label is not a feature; a booked inquiry is not returned after its booking | Pass |
| Same seed, same metrics; a better model replaces production; an impossible gate gives "rejected" and production is unchanged | Pass |
| One score per open inquiry per day, bands High / Medium / Low, a rerun does not duplicate | Pass |
| Channel PSI is ok in Oct 2025 and an alert in May 2026, written to `etl.Alert` | Pass |
