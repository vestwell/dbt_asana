with milestones as(

    select
        project.project_id,
        m0.completed_at m0_done,
        m1.due_on m1_due,
        m1.completed_at m1_done,
        m2.due_on m2_due,
        m2.completed_at m2_done,
        m3.due_on m3_due,
        m3.completed_at m3_done,
        m3_inv.completed_at m3_investments,
        m3_is_elig.completed_at m3_is_final_elige_review,
        m3_rm_elig.completed_at m3_rm_eligibility_review,
        m3_rm_rem.completed_at m3_remainder,
        m4.due_on m4_due,
        m4.completed_at m4_done,
        m4_send_data.completed_at m4_sent,
        m5.due_on m5_due,
        m5.completed_at m5_done,

        --go from m0 to m5, the current milestone is the milestone corresponding to the first non-null completed_at value, else null (current milestone has added logic in the outermost query below)
        case
            when m0.completed_at is null
                then 'M0'
            when m1.completed_at is null
                then 'M1'
            when m2.completed_at is null
                then 'M2'
            when m3.completed_at is null
                then 'M3'
            when m4.completed_at is null
                then 'M4'
            when m5.completed_at is null
                then 'M5'
        end current_milestone

    from {{ ref('stg_asana__project') }} project
    left join (

        select
            project_task.project_id,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200479848126729' --and task._fivetran_deleted = False
        group by 1

        ) m0 on m0.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.due_date) due_on,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729715' --and task._fivetran_deleted = False
        group by 1

        ) m1 on m1.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.due_date) due_on,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729724' --and task._fivetran_deleted = False
        group by 1

        ) m2 on m2.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.due_date) due_on,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729737' --and task._fivetran_deleted = False
        group by 1

        ) m3 on m3.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729734' --and task._fivetran_deleted = False
        group by 1

        ) m3_inv on m3_inv.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729736' --and task._fivetran_deleted = False
        group by 1

        ) m3_is_elig on m3_is_elig.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729735' --and task._fivetran_deleted = False
        group by 1

        ) m3_rm_elig on m3_rm_elig.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729740' --and task._fivetran_deleted = False
        group by 1

        ) m3_rm_rem on m3_rm_rem.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.due_date) due_on,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729738' --and task._fivetran_deleted = False
        group by 1

        ) m4 on m4.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729739' --and task._fivetran_deleted = False
        group by 1

        ) m4_send_data on m4_send_data.project_id = project.project_id
    left join (

        select
            project_task.project_id,
            min(task.due_date) due_on,
            min(task.completed_at) completed_at

        from {{ ref('stg_asana__project_task') }} project_task
        join {{ ref('stg_asana__task') }} task on project_task.task_id = task.task_id
        join {{ ref('stg_asana__task_tag') }} task_tag on task_tag.task_id = task.task_id
        where task_tag.tag_id = '1200400519729743' --and task._fivetran_deleted = False
        group by 1

        ) m5 on m5.project_id = project.project_id

    where project.team_id = '1170823813530401'

)

select
    project_id,

    --
    case
        /*when task_data.percent_complete = '100%'
            then null*/
        when milestones.current_milestone is null
            then datediff(day, milestones.m5_done, current_timestamp)
        when milestones.current_milestone = 'M5'
            then datediff(day, milestones.m4_done, current_timestamp)
        when milestones.current_milestone = 'M4'
            then datediff(day, milestones.m3_done, current_timestamp)
        when milestones.current_milestone = 'M3'
            then datediff(day, milestones.m2_done, current_timestamp)
        when milestones.current_milestone = 'M2'
            then datediff(day, milestones.m1_done, current_timestamp)
        when milestones.current_milestone = 'M1'
            then datediff(day, milestones.m0_done, current_timestamp)
        /*when milestones.current_milestone = 'M0'
            then datediff(day, coalesce(clean_dates.psa_signed_date::timestamp, project.created_at), current_timestamp)
    */end days_in_current_phase,
/*
    --
    case
        when clean_dates.psa_signed_date is null
            then datediff(day, project.created_at, milestones.m0_done)
        else datediff(day, clean_dates.psa_signed_date::timestamp, milestones.m0_done)
    end psa_to_m0,

    --
    case
        when datediff(day, milestones.m0_done, milestones.m1_done) < 0
            then null
        else datediff(day, milestones.m0_done, milestones.m1_done)
    end m0_to_m1,

    --
    case
        when milestones.current_milestone in (null, 'm0', 'm1')
            then null
        when clean_dates.psa_signed_date is null
            then datediff(day, project.created_at, milestones.m1_done)
        else datediff(day, clean_dates.psa_signed_date::timestamp, milestones.m1_done)
    end psa_to_m1,
*/
    --
    case
        when milestones.current_milestone in (null, 'm0', 'm1', 'm2')
            then null
        when datediff(day, milestones.m1_done, milestones.m2_done) < 0
            then null
        else datediff(day, milestones.m1_done, milestones.m2_done)
    end m1_to_m2,

    --
    case
        when milestones.current_milestone in (null, 'm0', 'm1', 'm2', 'm3')
            then null
        else datediff(day, milestones.m2_done, milestones.m3_done)
    end m2_to_m3,

    --
    case
        when milestones.current_milestone in (null, 'm0', 'm1', 'm2', 'm3', 'm4')
            then null
        else datediff(day, milestones.m3_done, milestones.m4_done)
    end m3_to_m4,
/*
    datediff(day, milestones.m4_done, milestones.m5_done) m4_to_m5,
    datediff(day, milestones.m2_done, milestones.m5_done) m2_to_m5,
    datediff(day, clean_dates.first_expected_pay_date::timestamp, milestones.m5_done) expected_to_m5,
    project.created_at asana_created,*/
    milestones.m0_done,
    milestones.m1_due,
    milestones.m1_done,
    milestones.m2_due,

    --
    case
        when milestones.m2_due is null
            then 'Not set yet'
/*        when clean_dates.alignment_call_date is null
            then 'Set Portfolio Lvl'
        when datediff(day, clean_dates.alignment_call_date::timestamp, milestones.m2_due) = 0
            then null
*/        else 'Align Call Dates'
    end m2_aligned,

    milestones.m2_done,
    milestones.m3_investments,
    milestones.m3_rm_eligibility_review,
    milestones.m3_is_final_elige_review,
    milestones.m3_remainder,
    milestones.m3_due,

    --
    case
        when milestones.m3_due is null
            then 'Not set yet'
/*        when clean_dates.employee_invites_date is null
            then 'Set Portfolio Lvl'
        when datediff(day, clean_dates.employee_invites_date::timestamp, milestones.m3_due) = 0
            then null
*/        else 'Align Call Dates'
    end m3_aligned,

    milestones.m3_done,
    milestones.m4_sent,
--    datediff(day, clean_dates.psa_signed_date::timestamp, milestones.m4_sent) close_to_send,
    milestones.m4_due,
    null m4_aligned,
    milestones.m4_done,
    milestones.m5_due,
    null m5_aligned,
    milestones.m5_done

from milestones
