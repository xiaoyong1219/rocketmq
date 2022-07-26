package com.george.mqproservice.util;

import java.util.Iterator;
import java.util.List;

/**
 * @title: ListSplitter.java
 * @description: LIST对象进行分割
 * @author: George
 * @date: 2022/7/26 18:19
 */
public class ListSplitter<T> implements Iterator<List<T>> {
    /**
     * 所有内容
     */
    private final List<T> ts;

    /**
     * 多少数据进行分割
     */
    private int size;

    /**
     * 已经分割的索引
     */
    private int currIndex;

    /**
     * 构造方法
     *
     * @param ts   所有内容
     * @param size 分割数量
     */
    public ListSplitter(List<T> ts, int size) {
        this.ts = ts;
        this.size = size;
    }

    /**
     * 实现是否还厚后续数据
     *
     * @return true:有 false:无
     */
    @Override
    public boolean hasNext() {
        return currIndex < ts.size();
    }

    @Override
    public List<T> next() {
        int nextIndex = currIndex;
        int totalSize = 0;
        for (; nextIndex < ts.size(); nextIndex++) {
            T t = ts.get(nextIndex);
            totalSize = totalSize + t.toString().length();
            if (totalSize > size) {
                break;
            }
        }
        List<T> subList = ts.subList(currIndex, nextIndex);
        currIndex = nextIndex;
        return subList;
    }
}