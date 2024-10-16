with task as (
    select task.*,
    coalesce(parent4.parent_task_id, parent3.parent_task_id, parent2.parent_task_id, parent.parent_task_id, task.parent_task_id) as master_parent_id

    from {{ var('task') }} task
    left join {{ ref('stg_asana__task') }} parent on parent.task_id = task.parent_task_id
    left join {{ ref('stg_asana__task') }} parent2 on parent2.task_id = parent.parent_task_id
    left join {{ ref('stg_asana__task') }} parent3 on parent3.task_id = parent2.parent_task_id
    left join {{ ref('stg_asana__task') }} parent4 on parent4.task_id = parent3.parent_task_id
),

task_comments as (

    select *
    from {{ ref('int_asana__task_comments') }}
),

task_followers as (

    select *
    from {{ ref('int_asana__task_followers') }}
),

task_open_length as (

    select *
    from {{ ref('int_asana__task_open_length') }}
),

task_tags as (

    select *
    from {{ ref('int_asana__task_tags') }}
),

task_assignee as (

    select *
    from  {{ ref('int_asana__task_assignee') }}
    where has_assignee
),

task_projects as (

    select *
    from {{ ref('int_asana__task_projects') }}
),

project_tasks as (

    select *
    from {{ ref('stg_asana__project_task') }}
),

subtask_parent as (

    select *
    from {{ ref('int_asana__subtask_parent') }}

),

task_first_modifier as (

    select *
    from {{ ref('int_asana__task_first_modifier') }}
),

users as (

    select
        user_id,
        user_name

    from {{ ref('stg_asana__user') }}
),

task_join as (

    select
        task.*,
        users.user_name as completed_by_name,
        concat('https://app.asana.com/0/0/', task.task_id) as task_link,
        task_assignee.assignee_name,
        task_assignee.assignee_email,

        task_open_length.days_open,
        task_open_length.is_currently_assigned,
        task_open_length.has_been_assigned,
        task_open_length.days_since_last_assignment, -- is null for never-assigned tasks
        task_open_length.days_since_first_assignment, -- is null for never-assigned tasks
        task_open_length.last_assigned_at,
        task_open_length.first_assigned_at,

        task_first_modifier.first_modifier_user_id,
        task_first_modifier.first_modifier_name,

        task_comments.conversation,
        coalesce(task_comments.number_of_comments, 0) as number_of_comments,
        task_followers.followers,
        coalesce(task_followers.number_of_followers, 0) as number_of_followers,
        task_tags.tags,
        coalesce(task_tags.number_of_tags, 0) as number_of_tags,

        project_tasks.project_id,
        task_projects.projects_sections,

        subtask_parent.subtask_id is not null as is_subtask, -- parent id is in task.*
        subtask_parent.parent_task_name,
        subtask_parent.parent_assignee_user_id,
        subtask_parent.parent_assignee_name,
        subtask_parent.parent_due_date,
        subtask_parent.parent_created_at

    from
    task
    join task_open_length on task.task_id = task_open_length.task_id
    left join task_first_modifier on task.task_id = task_first_modifier.task_id

    left join task_comments on task.task_id = task_comments.task_id
    left join task_followers on task.task_id = task_followers.task_id
    left join task_tags on task.task_id = task_tags.task_id

    left join task_assignee on task.task_id = task_assignee.task_id

    left join subtask_parent on task.task_id = subtask_parent.subtask_id

    left join task_projects on task.task_id = task_projects.task_id
    left join project_tasks on project_tasks.task_id = coalesce(task.master_parent_id, task.task_id)

    left join users on users.user_id = task.completed_by_user_id

)

select * from task_join
