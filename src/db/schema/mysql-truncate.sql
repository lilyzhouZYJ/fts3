TRUNCATE t_job_backup;
TRUNCATE t_file_backup;
TRUNCATE t_stage_req;
TRUNCATE t_file;
TRUNCATE t_job_share_config;
TRUNCATE t_job;
TRUNCATE t_vo_acl;
TRUNCATE t_se_pair_acl;
TRUNCATE t_bad_dns;
TRUNCATE t_bad_ses;
TRUNCATE t_share_config;
TRUNCATE t_link_config;
TRUNCATE t_group_members;
TRUNCATE t_se_acl;
TRUNCATE t_se;
TRUNCATE t_credential;
TRUNCATE t_credential_cache;
TRUNCATE t_debug;
TRUNCATE t_config_audit;
TRUNCATE t_optimize;
TRUNCATE t_optimizer_evolution;
TRUNCATE t_server_config;
INSERT INTO t_server_config (retry,max_time_queue) values(0,0);
