package com.example.sparkpomrob.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Date;

@Data
@AllArgsConstructor
public class RowDTO implements Serializable {
    private String HotelAddress;
    private long Additional_Number_of_Scoring;
    private Date Review_Date;

    private double Average_Score;

    private String HotelName;

    private String Reviewer_Nationality;

    private String NegativeReview;

    private long Review_Total_Negative_Word_Counts;

    private long Total_Number_of_Reviews;

    private String Positive_Review;

    private long Review_Total_Positive_Word_Counts;

    private long Total_Number_of_Reviews_Reviewer_Has_Given;

    private double Reviewer_Score;

    private String tags;

    private String days_since_review;

    private String lat;

    private String lng;

    public static RowDTO convertFromRow(Row row){
        return new RowDTO(
                row.getString(0),
               row.getInt(1),
                new Date(row.getString(2)),
                row.getDouble(3),
                row.getString(4),
                row.getString(5), // rev nationality
                row.getString(6),
                row.getInt(7),
                row.getInt(8),
                row.getString(9), // Positive Review
                row.getInt(10),
                row.getInt(11), // total numb. of  reviews.
                row.getDouble(12), //rew score
                row.getString(13), //tags
                row.getString(14),
                row.getString(15),
                row.getString(16)
        );

    }
}
