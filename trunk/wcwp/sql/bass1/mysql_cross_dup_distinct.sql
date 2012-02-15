CREATE DEFINER=`root`@`localhost` PROCEDURE `NewProcedure`(`Param` int(11))
BEGIN
DECLARE done INT DEFAULT FALSE;
    declare v_product_no char(20);   
    declare v_opp_number char(20);   
    declare v_sms_cnt int(11); 
declare cur cursor for select product_no,opp_number,sms_cnt from cross_num;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = TRUE;
    open cur; 

  read_loop: LOOP
	    fetch cur into v_product_no,v_opp_number,v_sms_cnt;
    IF done THEN
      LEAVE read_loop;
    END IF;
	insert into cross_num_result (product_no,opp_number,sms_cnt)
	select 
	a,b,sms_cnt
	from 
	(
	select product_no a,opp_number b,sms_cnt sms_cnt
	from   cross_num where product_no = v_product_no and  opp_number  = v_opp_number 
	union all 
	select opp_number a,product_no b,sms_cnt
	from cross_num_result where opp_number = v_product_no and product_no = v_opp_number
	) t
	group by a,b,sms_cnt having count(0) = 1;
  END LOOP;
close cur;
END;
