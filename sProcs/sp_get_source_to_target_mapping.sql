CREATE procedure [cfg].[sp_get_source_to_target_mappings]
  @config_id integer,
  @return_length integer = 8
AS
BEGIN
  SET NOCOUNT ON;
  DECLARE @column_name varchar(100), @f_def varchar(1024), @p_list varchar(1024), @p_dtype varchar(1024);
	DECLARE @ret_source_query varchar(MAX) ='';
	DECLARE @raw_columns varchar(MAX) ='';
  DECLARE @map_construct varchar(MAX) = '"{x}"';
  DECLARE @ret_bkeys varchar(max) ='',
          @ret_bvals varchar(max) ='',
          @ret_dkeys varchar(max) = '',
          @ret_dvals varchar(max) = '',
          @ret_dtkeys varchar(max) = '',
          @ret_dtvals varchar(max) = '',
          @ret_nkeys varchar(max) = '',
          @ret_nvals varchar(max) = '',
          @ret_skeys varchar(max) = '',
          @ret_svals varchar(max) = '';
  DECLARE @b_count integer = 0,
          @d_count integer = 0,
          @dt_count integer = 0,
          @n_count integer = 0,
          @s_count integer = 0;
  DECLARE @db_cursor CURSOR;

	select @raw_columns=string_agg(cast('[' + source_column_name +']' as varchar(max)), ',') within group (order by custom_column_mapping_id asc)
	  from cfg.vw_raw_to_curated_column_mapping 
		where  config_id=@config_id and active_ind=1 and (derived_ind=0 or (derived_ind=1 and transformation_function_id is null)); --orig columns + lookup columns

	select @ret_source_query = string_agg(cast(replace('cast(Null as varchar) as {x}', '{x}', source_column_name) as varchar(max)), ', ') 
	      within group (order by custom_column_mapping_id asc)
    from data_management.cfg.vw_raw_to_curated_column_mapping
    where config_id=@config_id and active_ind=1 and derived_ind=1 and transformation_function_id is not null;
  IF len(@ret_source_query) > 0
	  BEGIN
      select @ret_source_query = 'SELECT ' + @raw_columns + ', ' + @ret_source_query + ' FROM ' + source_database_name + '.'+source_schema_name + '.[' + source_table_name + '] WHERE AUDIT_ACTIVE_ROW_IND=\''Y\'''
        from data_management.cfg.source_to_output_config where config_id=@config_id;  
    END
  ELSE
	  BEGIN
      select @ret_source_query = 'SELECT ' + @raw_columns + ' FROM ' + source_database_name + '.'+source_schema_name + '.[' + source_table_name + '] WHERE AUDIT_ACTIVE_ROW_IND=\''Y\'''
        from data_management.cfg.source_to_output_config where config_id=@config_id;
    END
  set @db_cursor = CURSOR FOR
    WITH CTE AS
    (select custom_column_mapping_id, source_column_name, replace(transformation_function_def,'i0', source_column_name) as f_definition, transformation_parameter_text as f_parameter_text, transform_func_data_type as transform_data_type
     from data_management.cfg.vw_raw_to_curated_column_mapping
     WHERE active_ind=1 and config_id=@config_id AND transformation_function_id is not null)
    SELECT source_column_name, f_definition, f_parameter_text, transform_data_type
    FROM CTE order by custom_column_mapping_id;

  --construct each return variables for each data types (Boolean, Date, Datetime, Numeric, String)
  OPEN @db_cursor FETCH @db_cursor INTO @column_name, @f_def, @p_list, @p_dtype;
  WHILE @@FETCH_STATUS = 0
  BEGIN         
    DECLARE @c_name varchar(100), @c_pos as VARCHAR(100);
    DECLARE @column_cursor CURSOR;
    set @column_cursor = CURSOR FOR 
      SELECT VALUE as C_NAME, 'i' + cast(ordinal as varchar) as POSITION
        from string_split(@p_list, '|',1);

    -- contruct each function by replace the place holders (i1~in) with column names
    OPEN @column_cursor FETCH @column_cursor INTO @c_name, @c_pos;
    WHILE @@FETCH_STATUS = 0
    BEGIN
      SET @f_def = replace(@f_def, @c_pos, @c_name);
      FETCH NEXT FROM @column_cursor into @c_name, @c_pos;
    END

    if @p_dtype = 'Boolean'
    begin
      set @b_count = @b_count + 1
      set @ret_bkeys = @ret_bkeys + ';' + replace(@map_construct, '{x}', @column_name);
      set @ret_bvals = @ret_bvals + ';' + replace(@map_construct, '{x}', @f_def);
    end 
    if @p_dtype = 'Date'
    begin
      set @d_count = @d_count + 1
      set @ret_dkeys = @ret_dkeys + ';' + replace(@map_construct, '{x}', @column_name);
      set @ret_dvals = @ret_dvals + ';' + replace(@map_construct, '{x}', @f_def);
    end
    if @p_dtype in ('Timestamp','DateTime')
    begin
      set @dt_count = @dt_count + 1
      set @ret_dtkeys = @ret_dtkeys + ';' + replace(@map_construct, '{x}', @column_name);
      set @ret_dtvals = @ret_dtvals + ';' + replace(@map_construct, '{x}', @f_def);
    end
    if @p_dtype in ('Numeric', 'Decimal', 'Integer', 'Bit')
    begin
      set @n_count = @n_count + 1
      set @ret_nkeys = @ret_nkeys + ';' + replace(@map_construct, '{x}', @column_name);
      set @ret_nvals = @ret_nvals + ';' + replace(@map_construct, '{x}', @f_def);
    end
    if @p_dtype = 'String'
    begin
      set @s_count = @s_count + 1
      set @ret_skeys = @ret_skeys + ';' + replace(@map_construct, '{x}', @column_name);
      set @ret_svals = @ret_svals + ';' + replace(@map_construct, '{x}', @f_def);
    end

    FETCH NEXT FROM @db_cursor INTO @column_name, @f_def, @p_list, @p_dtype;
  END

  -- pad each varilable to @return_length
  WHILE (@b_count < @return_length)
  BEGIN
    set @b_count = @b_count + 1
    set @ret_bkeys = @ret_bkeys + ';"_b' + cast(@b_count as varchar) + '"'
    set @ret_bvals = @ret_bvals + ';"toBoolean(null())"'
  END

  WHILE (@d_count < @return_length)
  BEGIN
    set @d_count = @d_count + 1
    set @ret_dkeys = @ret_dkeys + ';"_d' + cast(@d_count as varchar) + '"'
    set @ret_dvals = @ret_dvals + ';"toDate(null())"'
  END

  WHILE (@dt_count < @return_length)
  BEGIN
    set @dt_count = @dt_count + 1
    set @ret_dtkeys = @ret_dtkeys + ';"_dt' + cast(@dt_count as varchar) + '"'
    set @ret_dtvals = @ret_dtvals + ';"toTimestamp(null())"'
  END

  WHILE (@n_count < @return_length)
  BEGIN
    set @n_count = @n_count + 1
    set @ret_nkeys = @ret_nkeys + ';"_n' + cast(@n_count as varchar) + '"'
    set @ret_nvals = @ret_nvals + ';"toDecimal(null())"'
  END

  WHILE (@s_count < @return_length)
  BEGIN
    set @s_count = @s_count + 1
    set @ret_skeys = @ret_skeys + ';"_s' + cast(@s_count as varchar) + '"'
    set @ret_svals = @ret_svals + ';"toString(null())"'
  END

  -- remove semi-column(;) from the beginning
  set @ret_bkeys=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_bkeys), '<START>;', ''), '<START>', ''))
  set @ret_bvals=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_bvals), '<START>;', ''), '<START>', ''))
  set @ret_dkeys=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_dkeys), '<START>;', ''), '<START>', ''))
  set @ret_dvals=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_dvals), '<START>;', ''), '<START>', ''))
  set @ret_dtkeys=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_dtkeys), '<START>;', ''), '<START>', ''))
  set @ret_dtvals=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_dtvals), '<START>;', ''), '<START>', ''))
  set @ret_nkeys=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_nkeys), '<START>;', ''), '<START>', ''))
  set @ret_nvals=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_nvals), '<START>;', ''), '<START>', ''))
  set @ret_skeys=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_skeys), '<START>;', ''), '<START>', ''))
  set @ret_svals=TRIM(REPLACE(REPLACE(trim('<START>' + @ret_svals), '<START>;', ''), '<START>', ''))

  -- return each variables with name changed
  SELECT @ret_source_query as source_query, @ret_bkeys as bkeys, @ret_bvals as bvals, @ret_dkeys as dkeys, @ret_dvals as dvals, @ret_dtkeys as dtkeys, @ret_dtvals as dtvals, @ret_nkeys as nkeys, @ret_nvals as nvals, @ret_skeys as skeys, @ret_svals as svals; 
END
