package com.olxpbenchmark.benchmarks.subenchmark.pojo;

public class Item {

    public int i_id; // PRIMARY KEY
    public int i_im_id;
    public double i_price;
    public String i_name;
    public String i_data;

    @Override
    public String toString() {
        return ("\n***************** Item ********************"
                + "\n*    i_id = " + i_id + "\n*  i_name = " + i_name
                + "\n* i_price = " + i_price + "\n*  i_data = " + i_data
                + "\n* i_im_id = " + i_im_id + "\n**********************************************");
    }

} // end Item