{{ config(
    materialized = 'table',
    schema = 'composite'
)}}

select
    TASK_ID,
    TASK_NAME,
    regexp_substr(task_name, '[-+]?[0-9]*\.?[0-9]+'),
    CONVERSATION,
    TAGS,
    TASK_DESCRIPTION,
    ASSIGNEE_USER_ID,
    ASSIGNEE_NAME,
    PROJECT_ID,
    IS_COMPLETED,
    COMPLETED_AT,
    COMPLETED_BY_USER_ID,
    CREATED_AT,
    DUE_DATE,
    FIRST_MODIFIER_NAME,
    MODIFIED_AT,
    CUSTOM_TASK_MINS,
    TASK_LINK,
    DAYS_OPEN,
    IS_CURRENTLY_ASSIGNED,
    HAS_BEEN_ASSIGNED,
    DAYS_SINCE_LAST_ASSIGNMENT,
    DAYS_SINCE_FIRST_ASSIGNMENT,
    LAST_ASSIGNED_AT,
    FIRST_ASSIGNED_AT,
    NUMBER_OF_COMMENTS,
    CUSTOM_VW_PLAN_ID,
    CUSTOM_ENTERPRISE,
    CUSTOM_SH_TYPE,
    CUSTOM_CURRENT_YEAR,
    CUSTOM_PRIOR_YEAR,
    CUSTOM_MISSING_COMPENSATION,
    CUSTOM_OTHER_PLAN,
    CUSTOM_402_G_LIMIT,
    CUSTOM_415_LIMIT,
    CUSTOM_ACTIVE_ELIG_12_31_EOY_,
    CUSTOM_TH_THIS_YEAR,
    CUSTOM_MATCH_CAL_PERIOD,
    CUSTOM_PROFIT_SHARING,
    CUSTOM_MATCH,
    CUSTOM_DATE_SENT_YYYY_MM_DD_,
    CUSTOM_ADP_TEST_RESULT,
    CUSTOM_ACP_TEST_RESULT,
    CUSTOM_PRIOR_YEAR_ADP,
    CUSTOM_PRIOR_YEAR_ACP,
    CUSTOM_D_3_DISCREPANCIES,
    CUSTOM_TH_NEXT_YEAR,
    CUSTOM_FUNDING_DEADLINE_2022_MM_DD_

from {{ ref('asana__task') }}

--The below script allows for passing project_id's to filter the query.
{% if var('year_end_testing_project_ids') %}
where
    project_id in (
{{ var('year_end_testing_project_ids') | join (", ") }}
)
{% endif %}

--add custom field pass throughs
