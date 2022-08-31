with task_project as (

    select *
    from {{ var('project_task') }}

),

project as (

    select *
    from {{ var ('project') }}
),

all_tasks as (

   select
        task.task_id,
        coalesce(parent4.parent_task_id, parent3.parent_task_id, parent2.parent_task_id, parent.parent_task_id, task.parent_task_id) as master_parent_id

    from {{ var('task') }}  task
    left join {{ ref('stg_asana__task') }} parent on parent.task_id = task.parent_task_id
    left join {{ ref('stg_asana__task') }} parent2 on parent2.task_id = parent.parent_task_id
    left join {{ ref('stg_asana__task') }} parent3 on parent3.task_id = parent2.parent_task_id
    left join {{ ref('stg_asana__task') }} parent4 on parent4.task_id = parent3.parent_task_id
),

task_section as (

    select
        task_section.section_id,
        all_tasks.task_id
        
    from all_tasks
    join {{ var('task_section') }} task_section on coalesce(all_tasks.master_parent_id, all_tasks.task_id) = task_section.task_id


),

section as (

    select *
    from {{ var ('section') }}

),

task_project_section as (

    select
        all_tasks.task_id,
        project.project_name || (case when section.section_name = '(no section)' then ''
            else ': ' || section.section_name end) as project_section,
        project.project_id
    from
    all_tasks
    join task_project on coalesce(all_tasks.master_parent_id, all_tasks.task_id) = task_project.task_id
    join project
        on project.project_id = task_project.project_id
    join task_section
        on task_section.task_id = task_project.task_id
    join section
        on section.section_id = task_section.section_id
        and section.project_id = project.project_id
),

agg_project_sections as (
    select
        task_id,
        {{ fivetran_utils.string_agg( 'task_project_section.project_section', "', '" )}} as projects_sections,
        count(project_id) as number_of_projects

    from task_project_section

    group by 1
)

select * from agg_project_sections
