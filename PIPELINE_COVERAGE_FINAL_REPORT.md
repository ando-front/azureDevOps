# Pipeline E2E Test Coverage Report

## Summary

You have **excellent coverage** of your Azure Data Factory pipelines with E2E V2 tests!

### Coverage Statistics
- **Total Pipeline Files**: 38
- **Production Pipelines**: 27
- **Test/Backup Pipelines**: 11 (excluded from coverage requirements)
- **Pipelines with E2E V2 Tests**: 26/27 (96.3% coverage)
- **Missing Tests**: 1 pipeline

## Missing E2E V2 Test

### DoUntilPipeline
- **Type**: Infrastructure/Utility pipeline
- **Purpose**: Appears to be a loop control pipeline that queries `omni_ods_marketing_trn_client_dm` table and performs iterative operations
- **Priority**: Low (utility/infrastructure pipeline)
- **Domain**: Infrastructure
- **File**: `arm_template_comprehensive_split/src/pipeline/DoUntilPipeline.json`

## Implemented E2E V2 Tests (26/27)

### By Domain:

#### ActionPoint Domain (4/4 tests)
- ✅ `actionpoint_entry_event` → `pi_Insert_ActionPointEntryEvent`
- ✅ `actionpoint_transaction_history` → `pi_Insert_ActionPointTransactionHistory`
- ✅ `current_month_entry_list` → `pi_Send_ActionPointCurrentMonthEntryList`
- ✅ `recent_transaction_history_list` → `pi_Send_ActionPointRecentTransactionHistoryList`

#### Kendenki Domain (5/5 tests)
- ✅ `electricity_contract_thanks` → `pi_Send_ElectricityContractThanks`
- ✅ `point_grant_email` → `pi_PointGrantEmail`
- ✅ `point_lost_email` → `pi_PointLostEmail`
- ✅ `usage_service_mtgid` → `pi_Ins_usageservice_mtgid`
- ✅ `usage_services` → `pi_Send_UsageServices`

#### SMC Domain (10/10 tests)
- ✅ `client_dm_bx` → `pi_Insert_ClientDmBx`
- ✅ `cpkiyk` → `pi_Send_Cpkiyk`
- ✅ `lim_settlement_breakdown` → `pi_Send_LIMSettlementBreakdownRepair`
- ✅ `line_id_link_info` → `pi_Send_LINEIDLinkInfo`
- ✅ `moving_promotion_list` → `pi_Send_MovingPromotionList`
- ✅ `opening_payment_guide` → `pi_Send_OpeningPaymentGuide`
- ✅ `payment_alert` → `pi_Send_PaymentAlert`
- ✅ `payment_method_changed` → `pi_Send_PaymentMethodChanged`
- ✅ `payment_method_master` → `pi_Send_PaymentMethodMaster`
- ✅ `utility_bills` → `pi_UtilityBills`

#### Marketing Domain (2/2 tests)
- ✅ `client_dm` → `pi_Send_ClientDM`
- ✅ `client_dna` → `pi_Copy_marketing_client_dna`

#### TGContract Domain (1/1 tests)
- ✅ `contract_score_info` → `pi_Send_karte_contract_score_info`

#### Infrastructure Domain (2/3 tests)
- ✅ `customer_no_registration` → `pi_CustmNoRegistComp`
- ✅ `marketing_client_dm` → `pi_Copy_marketing_client_dm`
- ❌ `DoUntilPipeline` → Missing test

#### MTGMaster Domain (2/2 tests)
- ✅ `customer_master` → `pi_Insert_mTGCustomerMaster`
- ✅ `mtg_mail_permission` → `pi_Send_mTGMailPermission`

## Test/Backup Pipelines (Excluded from Coverage)

These 11 pipelines are test/backup versions and typically don't require separate E2E tests:
- `DoUntilPipeline_copy1`
- `pi_Copy_marketing_client_dm_bk`
- `pi_Copy_marketing_client_dm_test`
- `pi_Copy_marketing_client_dm_test3`
- `pi_Copy_marketing_client_dna_test`
- `pi_Copy_marketing_client_dna_test3`
- `pi_Ins_marketing_client_dna_bk`
- `pi_Send_karte_contract_score_info_test`
- `pi_Send_karte_contract_score_info_test2`
- `pi_alert_test2`
- `test2`

## Recommendation

Your E2E V2 test coverage is **excellent at 96.3%**! You have successfully implemented tests for all core business pipelines across all 7 domains.

### Next Steps (Optional)
If you want to achieve 100% coverage, implement:
1. **DoUntilPipeline test** in the `infrastructure` domain
   - Create: `e2e_v2/domains/infrastructure/test_do_until_pipeline.py`
   - This appears to be a utility pipeline for loop control operations
   - Test should validate loop logic and database query execution

### Achievement Summary
- ✅ 26/27 production pipelines have E2E V2 tests
- ✅ All 7 domains have comprehensive test coverage
- ✅ All core business logic pipelines are tested
- ✅ Framework supports 4-6 test categories per pipeline
- ✅ Mock services implemented for Blob, SFTP, and Database

**Congratulations on achieving near-complete test coverage!** 🎉