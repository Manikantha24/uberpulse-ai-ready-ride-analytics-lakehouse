# UberPulse — Data Quality Rules

## 1. Hard validation failures

These rules currently cause a record to be quarantined:

* Pickup timestamp is missing.
* Drop-off timestamp is missing.
* Trip duration is zero or negative.
* Trip distance is negative.
* Fare amount is missing.
* Total amount is missing.

## 2. Warnings — do not automatically quarantine

The following values require investigation or business justification before rejection:

* Zero trip distance.
* Negative fare or total amount.
* Unusually high fare amounts.
* Unusually large trip distances.
* Trips lasting more than 24 hours.
* Missing passenger count, rate code, or other optional fields.

## 3. Processing principles

* Preserve the raw dataset unchanged.
* Keep validation reasons for quarantined records.
* Do not silently discard records.
* Record warning conditions separately from hard failures.
* Define and test thresholds before applying them.
* Reconcile record counts after every processing run.

## 4. Current limitations

* Warning conditions have not yet been implemented in the validation script.
* Fare and distance thresholds are not yet finalized.
* Business-specific exceptions have not yet been defined.
* The current implementation runs locally using pandas; cloud processing and orchestration are future stages.
