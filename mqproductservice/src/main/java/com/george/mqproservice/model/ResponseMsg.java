package com.george.mqproservice.model;

import lombok.Data;

/**
 * @title: ResponseMsg.java
 * @description: 响应
 * @author: George
 * @date: 2022/7/26 17:00
 */
@Data
public class ResponseMsg {
    public static final int CODE_FAIL = 500;

    public static final int CODE_SUCCESS = 200;

    public static final String MSG_SUCCESS = "success";

    public static final String MSG_FAIL = "fail";

    private int code;

    private String msg;

    private Object data;

    public ResponseMsg() {
        this.code = CODE_FAIL;
        this.msg = MSG_FAIL;
    }

    public void setSuccessData(Object data) {
        this.code = CODE_SUCCESS;
        this.msg = MSG_SUCCESS;
        this.data = data;
    }
}