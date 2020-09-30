package com.fox.generator.entity;

import com.fox.generator.Number;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NumberPojo {
    private int numberOne;
    private int numberTwo;

    public static NumberPojo getFromAvro(Number number) {
        return new NumberPojo(number.getNumberOne(), number.getNumberTwo());
    }
}
