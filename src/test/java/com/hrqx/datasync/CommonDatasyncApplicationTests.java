package com.hrqx.datasync;

import com.hrqx.datasync.common.connection.SqlConn;
import com.hrqx.datasync.common.utils.DataSyncUtil;
import com.hrqx.datasync.modules.base.dto.DataSyncDto;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class CommonDatasyncApplicationTests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Autowired
	private SqlConn sqlConn;

	@Test
	public void contextLoads() {

		String dataSyncPath = System.getProperty("user.dir") + "/src/main/resources/databaseSync.xml";
		String replacePath = System.getProperty("user.dir") + "/src/main/resources/replaceRule.xml";
		String filterPath = System.getProperty("user.dir") + "/src/main/resources/filterRule.xml";
		DataSyncDto dataSyncDto = new DataSyncDto(dataSyncPath,replacePath,filterPath,4);
		DataSyncUtil.dataBaseSync(dataSyncDto,jdbcTemplate,sqlConn);
	}

}
