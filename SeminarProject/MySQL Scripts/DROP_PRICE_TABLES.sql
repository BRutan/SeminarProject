CREATE PROCEDURE CLEARPRICETABLES()
BEGIN
	DECLARE @tables text;
    DECLARE curs CURSOR FOR SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_SCHEMA = "corp_twitter_data" AND TABLE_NAME LIKE "%prices%";
    

END

