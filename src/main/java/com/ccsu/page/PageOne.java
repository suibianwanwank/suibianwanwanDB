package com.ccsu.page;


import com.ccsu.log.UndoLog;
import com.ccsu.utils.Parser;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.ccsu.page.PageManager.PAGE_SIZE;

/*

/leafFlag/  /tableName/  /ROOT_OFFSET/ /FIELD_NUMBER/  /FIELD_MESSAGE_SIZE/ /FIELD_MESSAGE_SIZE/.....
    1           40             8              4                 40
 */
public class PageOne {

    private static final int INIT_CHECK_OFFSET =0 ;
    private static final int END_CHECK_OFFSET =8 ;
    private static final int CHECK_LENGTH =8 ;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PAGE_SIZE];
//        setVcOpen(raw);
        return raw;
    }
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);
        setVcOpen(pg.getData());
    }


    private static void setVcOpen(byte[] raw) {
        //文件异常关闭校验
        System.arraycopy(Parser.unParseLong(System.currentTimeMillis()), 0, raw, INIT_CHECK_OFFSET, CHECK_LENGTH);
    }

    private static void transPortVcOpen(byte[] raw) {
        //文件正常关闭，拷贝数组
        System.arraycopy(raw, INIT_CHECK_OFFSET, raw, END_CHECK_OFFSET, CHECK_LENGTH);
    }


    public static void close(Page pg) {
        pg.setDirty(true);
        transPortVcOpen(pg.getData());
    }

    public static boolean check(Page pageOne) {
        //检查校验
        byte[] data = pageOne.getData();
        long initCheckNum = Parser.parseLong(Arrays.copyOfRange(data, INIT_CHECK_OFFSET, END_CHECK_OFFSET));
        long endCheckNum = Parser.parseLong(Arrays.copyOfRange(data, END_CHECK_OFFSET, END_CHECK_OFFSET+8));
        return initCheckNum==endCheckNum;
    }
}
