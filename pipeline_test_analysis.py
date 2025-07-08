#!/usr/bin/env python3
"""
Pipeline Test Analysis Script
Identifies which pipelines are missing E2E V2 tests
"""

import os
import json
from pathlib import Path

# All pipeline files (38 total)
all_pipelines = [
    "DoUntilPipeline",
    "DoUntilPipeline_copy1", 
    "pi_Copy_marketing_client_dm",
    "pi_Copy_marketing_client_dm_bk",
    "pi_Copy_marketing_client_dm_test",
    "pi_Copy_marketing_client_dm_test3",
    "pi_Copy_marketing_client_dna",
    "pi_Copy_marketing_client_dna_test",
    "pi_Copy_marketing_client_dna_test3",
    "pi_CustmNoRegistComp",
    "pi_Ins_marketing_client_dna_bk",
    "pi_Ins_usageservice_mtgid",
    "pi_Insert_ActionPointEntryEvent",
    "pi_Insert_ActionPointTransactionHistory",
    "pi_Insert_ClientDmBx",
    "pi_Insert_mTGCustomerMaster",
    "pi_PointGrantEmail",
    "pi_PointLostEmail",
    "pi_Send_ActionPointCurrentMonthEntryList",
    "pi_Send_ActionPointRecentTransactionHistoryList",
    "pi_Send_ClientDM",
    "pi_Send_Cpkiyk",
    "pi_Send_ElectricityContractThanks",
    "pi_Send_LIMSettlementBreakdownRepair",
    "pi_Send_LINEIDLinkInfo",
    "pi_Send_MovingPromotionList",
    "pi_Send_OpeningPaymentGuide",
    "pi_Send_PaymentAlert",
    "pi_Send_PaymentMethodChanged",
    "pi_Send_PaymentMethodMaster",
    "pi_Send_UsageServices",
    "pi_Send_karte_contract_score_info",
    "pi_Send_karte_contract_score_info_test",
    "pi_Send_karte_contract_score_info_test2",
    "pi_Send_mTGMailPermission",
    "pi_UtilityBills",
    "pi_alert_test2",
    "test2"
]

# Define the production pipelines (excluding test/backup versions)
production_pipelines = [
    "DoUntilPipeline",  # Infrastructure pipeline 
    "pi_Copy_marketing_client_dm",
    "pi_Copy_marketing_client_dna", 
    "pi_CustmNoRegistComp",
    "pi_Ins_usageservice_mtgid",
    "pi_Insert_ActionPointEntryEvent",
    "pi_Insert_ActionPointTransactionHistory", 
    "pi_Insert_ClientDmBx",
    "pi_Insert_mTGCustomerMaster",
    "pi_PointGrantEmail",
    "pi_PointLostEmail",
    "pi_Send_ActionPointCurrentMonthEntryList",
    "pi_Send_ActionPointRecentTransactionHistoryList",
    "pi_Send_ClientDM",
    "pi_Send_Cpkiyk",
    "pi_Send_ElectricityContractThanks",
    "pi_Send_LIMSettlementBreakdownRepair",
    "pi_Send_LINEIDLinkInfo",
    "pi_Send_MovingPromotionList",
    "pi_Send_OpeningPaymentGuide",
    "pi_Send_PaymentAlert",
    "pi_Send_PaymentMethodChanged",
    "pi_Send_PaymentMethodMaster",
    "pi_Send_UsageServices",
    "pi_Send_karte_contract_score_info",
    "pi_Send_mTGMailPermission",
    "pi_UtilityBills"
]

# Test/backup versions that may not need production tests
test_backup_pipelines = [
    "DoUntilPipeline_copy1",
    "pi_Copy_marketing_client_dm_bk",
    "pi_Copy_marketing_client_dm_test",
    "pi_Copy_marketing_client_dm_test3",
    "pi_Copy_marketing_client_dna_test",
    "pi_Copy_marketing_client_dna_test3",
    "pi_Ins_marketing_client_dna_bk",
    "pi_Send_karte_contract_score_info_test",
    "pi_Send_karte_contract_score_info_test2",
    "pi_alert_test2",
    "test2"
]

# Define implemented E2E V2 tests with their pipeline mappings
implemented_tests = {
    # ActionPoint Domain
    "actionpoint_entry_event": "pi_Insert_ActionPointEntryEvent",
    "actionpoint_transaction_history": "pi_Insert_ActionPointTransactionHistory",
    "current_month_entry_list": "pi_Send_ActionPointCurrentMonthEntryList",
    "recent_transaction_history_list": "pi_Send_ActionPointRecentTransactionHistoryList",
    
    # Kendenki Domain
    "point_grant_email": "pi_PointGrantEmail",
    "point_lost_email": "pi_PointLostEmail",
    "usage_service_mtgid": "pi_Ins_usageservice_mtgid",
    "usage_services": "pi_Send_UsageServices",
    "electricity_contract_thanks": "pi_Send_ElectricityContractThanks",
    
    # SMC Domain
    "payment_alert": "pi_Send_PaymentAlert",
    "payment_method_changed": "pi_Send_PaymentMethodChanged",
    "payment_method_master": "pi_Send_PaymentMethodMaster",
    "utility_bills": "pi_UtilityBills",
    "client_dm_bx": "pi_Insert_ClientDmBx",
    "lim_settlement_breakdown": "pi_Send_LIMSettlementBreakdownRepair",
    "line_id_link_info": "pi_Send_LINEIDLinkInfo",
    "moving_promotion_list": "pi_Send_MovingPromotionList",
    "opening_payment_guide": "pi_Send_OpeningPaymentGuide",
    "cpkiyk": "pi_Send_Cpkiyk",
    
    # Marketing Domain
    "client_dm": "pi_Send_ClientDM",
    "client_dna": "pi_Copy_marketing_client_dna",
    
    # TGContract Domain
    "contract_score_info": "pi_Send_karte_contract_score_info",
    
    # Infrastructure Domain
    "marketing_client_dm": "pi_Copy_marketing_client_dm",
    "customer_no_registration": "pi_CustmNoRegistComp",
    
    # MTGMaster Domain
    "customer_master": "pi_Insert_mTGCustomerMaster",
    "mtg_mail_permission": "pi_Send_mTGMailPermission",
    
    # Missing DoUntilPipeline - needs to be implemented
}

def analyze_pipeline_coverage():
    """Analyze which pipelines have tests implemented"""
    
    print("=" * 80)
    print("PIPELINE E2E TEST COVERAGE ANALYSIS")
    print("=" * 80)
    
    # Get list of pipelines with tests
    tested_pipelines = set(implemented_tests.values())
    
    print(f"\nTotal Pipeline Files: {len(all_pipelines)}")
    print(f"Production Pipelines: {len(production_pipelines)}")
    print(f"Test/Backup Pipelines: {len(test_backup_pipelines)}")
    print(f"Pipelines with E2E V2 Tests: {len(tested_pipelines)}")
    print(f"Missing Tests: {len(production_pipelines) - len(tested_pipelines)}")
    
    # Find missing pipelines
    missing_pipelines = []
    for pipeline in production_pipelines:
        if pipeline not in tested_pipelines:
            missing_pipelines.append(pipeline)
    
    print(f"\n{'=' * 40}")
    print("MISSING E2E V2 TESTS ({} pipelines)".format(len(missing_pipelines)))
    print("=" * 40)
    
    if missing_pipelines:
        for i, pipeline in enumerate(missing_pipelines, 1):
            print(f"{i:2d}. {pipeline}")
    else:
        print("All pipelines have E2E V2 tests implemented!")
    
    print(f"\n{'=' * 40}")
    print("IMPLEMENTED E2E V2 TESTS ({} pipelines)".format(len(tested_pipelines)))
    print("=" * 40)
    
    # Group by domain
    domains = {
        "actionpoint": [],
        "kendenki": [],
        "smc": [],
        "marketing": [],
        "tgcontract": [],
        "infrastructure": [],
        "mtgmaster": []
    }
    
    for test_name, pipeline in implemented_tests.items():
        # Determine domain based on test file location
        test_file_path = f"e2e_v2/domains"
        for domain in domains.keys():
            if os.path.exists(f"{test_file_path}/{domain}/test_{test_name}.py"):
                domains[domain].append(f"{test_name} -> {pipeline}")
                break
    
    for domain, tests in domains.items():
        if tests:
            print(f"\n{domain.upper()} ({len(tests)} tests):")
            for test in sorted(tests):
                print(f"  • {test}")
    
    return missing_pipelines

def recommend_next_implementations(missing_pipelines):
    """Recommend which pipelines to implement next based on patterns"""
    
    print(f"\n{'=' * 40}")
    print("RECOMMENDATIONS FOR NEXT IMPLEMENTATIONS")
    print("=" * 40)
    
    # Group missing pipelines by likely domain
    recommendations = {
        "High Priority (Core Business Logic)": [],
        "Medium Priority (Support Functions)": [],
        "Low Priority (Utility/Infrastructure)": []
    }
    
    for pipeline in missing_pipelines:
        if any(x in pipeline for x in ["Send_", "Insert_", "Copy_"]):
            if "Marketing" in pipeline or "Client" in pipeline:
                recommendations["High Priority (Core Business Logic)"].append(pipeline)
            elif "ActionPoint" in pipeline or "Payment" in pipeline:
                recommendations["High Priority (Core Business Logic)"].append(pipeline)
            else:
                recommendations["Medium Priority (Support Functions)"].append(pipeline)
        else:
            recommendations["Low Priority (Utility/Infrastructure)"].append(pipeline)
    
    for priority, pipelines in recommendations.items():
        if pipelines:
            print(f"\n{priority}:")
            for pipeline in sorted(pipelines):
                print(f"  • {pipeline}")

if __name__ == "__main__":
    missing = analyze_pipeline_coverage()
    recommend_next_implementations(missing)