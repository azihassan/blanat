# Blanat Challenge - The challenge is now closed

Thank you all for your contributions. As of 1st of March 2024, this challenge is officially closed and we won't be actively maintaining it.

### Description

Blanat Challenge is a coding challenge designed to test your coding skills and your ability to write optimized code. [The challenge](#challenge-the-cheapest-city) primarily focuses on writing a program capable of efficiently handling large amounts of data and promptly returning the solution. It is open to everyone, and you can submit your code in any of the following languages: C, C++, Java, Rust, Python, PHP or JavaScript. This is your chance to demonstrate that your favorite programming language is the best and the fastest one! 😀

> This challenge is part of BlaBlaConf 2024, Make sure to check the [event](https://blablaconf.com)

### Contents

- [Contents](#contents)
- [Submit your solution](#submit-your-solution)
- [Challenge: The Cheapest city](#challenge-the-cheapest-city)
  - [Constraints](#constraints)
  - [Output format](#output-format)
  - [Rules](#rules)
- [Evaluation Criteria](#evaluation-criteria)
- [Timeline](#timeline)
- [Leaderboard](#leaderboard)
- [Prize](#prize)
- [I have a question](#i-have-a-question)

## Submit your solution

- 1. Fork this repository
- 2. Create a folder under /submissions folder with your Github handle as name: `chermehdi` for example.
- 3. Read the challenge [carefully](#challenge-cheapest-city), rules and constraints and start coding with your favorite language. Java, C, C++, Rust, or JavaScript.
- 4. Optimize your code and make sure that it is working as expected.
- 5. Open a PR with the title `Challenge: Username submission` and wait for the review. and hopefully, your PR will be merged.
- 6. Our team will run your code with a large dataset(1 billion rows) and add your name to the leaderboard, along with the time it took to run your code.

## Challenge: The Cheapest city

We are in September 2022, the world has witnessed significant events over the past two years, like the global COVID-19 pandemic, the Russian invasion of Ukraine and the economic boycott imposed on Russia. These occurrences have had a profound and adverse impact on the international economy, leading to a surge in global inflation reminiscent of that in the 1920s.

As a result, people all over the world are dealing with the rising cost of living, which has hit unprecedented levels. Take Adnan, for example; he's a Moroccan citizen and, like many others, felt the impact. Luckily, Adnan turned Solo-preneur with the help of Djalal two years ago (Thanks Djalal) and now works remotely. This flexibility allows him to live anywhere in Morocco. That's when he got the idea to move to a city where the cost of products is more affordable since his wife isn’t against the idea 😁. Adnan starts analyzing rows of data **(data <= 1,000,000,000 rows)** to make a final decision. The data contains several Moroccan cities with the prices of different products.

Assist Adnan by writing a program, using any allowed language, to find the city with the lowest prices for products. **The challenge is to write an optimized code that can handle large amounts of data and quickly return the sorted results.**

<br>

### Constraints:

- 1.00 <= price <= 100.00
- we have 101 cities.
- We have 100 products.
- Each city may have the same product duplicated several times with different or identical prices.
- Your code will process a 10^9 (1 billion) line CSV file.
- You are allowed to code with any language from this list: C, C++, Java, Python, Rust, JavaScript, PHP.
- The input file is located on "input.txt"
- Make sure the result are saved in a file saved under `./output.txt`

### Output format

- Output has 6 lines.
- The first line contains the name of the cheapest city and the total price of products in that city, separated by one space.
- The remaining 5 lines display the first 5 cheapest products sorted by price in ascending order. In the case of two products with the same price, they are sorted alphabetically. For example, "Potato" and "Tomato" in the output below
- Don't print the same product twice. If you have multiple instances of the same product in the top 5 cheapest products, only print the cheapest one.
- Like the 1st line, other lines have one space between the product and the price.
- All the prices are decimal and they have a scale of 2.
- Be careful when you format the string that you will print, the spaces have to be respected, you will receive a **Wrong answer** even if the data is correct.
- Each line is trimmed (no spaces at the beginning or the end of each line).

### Rules

- Make sure that your code is compiling. 😄 It may seem trivial, but sometimes we receive code that doesn't compile.
- Your solution needs to **run under 15min**. Any submission taking more than that will be discarded.
- If your solution is correct, we will count it and add your name to the leaderboard, if not we will discard it.
- When you feel that your code is ready, create a folder under the "/submissions" folder with your GitHub handle as the name. Add the code to this folder and open a pull request (PR).
- Before creating the PR, your code has to produce the same result as the output below.
- You are allowed to submit the code several times with different langs.
- Only standard libraries provided by te language you picked can be used. No external/additional libs can be used
- Each code will be reviewed manually. Any fraudulent attempt will result in your account being banned, and you will be disqualified from the challenge.
- Java solutions should use `Main.java` and have a `public class Main` as a top-level class.
- Contestants are supposed to read input from the file in their current working directory named `input.txt` and write the output to a file in the same directory named `output.txt`.

<br>

### Testing

You can find an example of the input file [here](./input.txt). This file contains 1 million line to run your tests and validate your solution. The result contaning the correct answer from running the program against the 1M input file is saved in [this file](./output.txt)

You can also use the script found at `gen.py` to generate arbitrarily large test cases similar to the ones that we will use to validate your submission on the blanat server.

```
python3 gen.py --rows <number_of_rows>
```

> Note that to generate the 1B row file, it will require a couple of minutes to generate and would take around `~22Gb` of disk space.

### INPUT

Below are short examples for better understanding:

```csv
casa,tomato,6.23
casa,tomato,7.23
casa,tomato,8.23
casa,tomato,9.23
casa,potato,4.21
casa,flour,6.24
casa,oil,9.24
casa,oil,9.94
casa,oil,8.24
casa,sugar,5.23
casa,sugar,5.23
casa,sugar,5.23
casa,gaz,9.25
Tetouan,tomato,4.5
Tetouan,tomato,3.5
Tetouan,potato,3.5
Tetouan,flour,5.2
Tetouan,oil,7.7
Tetouan,oil,6.7
Tetouan,sugar,4.5
Tetouan,gaz,9.3
Tetouan,gaz,2.3
Tetouan,gaz,1.3
rabat,tomato,5.85
rabat,tomato,9.85
rabat,potato,4.65
rabat,flour,5.65
rabat,oil,8.15
rabat,oil,9.15
rabat,oil,7.15
rabat,sugar,9.05
rabat,sugar,9.15
rabat,gaz,9.45
```

<br>

### OUTPUT

```txt
Tetouan 48.50
gaz 1.30
potato 3.50
tomato 3.50
sugar 4.50
flour 5.20
```

**Explanation: Tetouan is the cheapest city because it has a total price of 48.50, while Casa has a total price of 93.73 and Rabat has a total price of 78.10.**

## Evaluation criteria

In short: Correctness and speed!
The competition focuses solely on correctness and speed of execution to gauge participants' ability to deliver accurate solutions within minimal computational time.

## Timeline

The challenge will run from the 19th of February 2024 until 29th of February 2024. The winner will be announced on March 1st 2024 in our social media challenges

## Leaderboard

> ⚠️ The leaderboard below is **temporary and not final**. We will go over the list below and perform another (manual) evaluation and a thorough review to validate it to respect the challenge constraints. If your name doesn't figure in this list, that means unfortunately the result of the evaluation is incorrect, and thus it is disqualified. No further review will be performed. Final results, and winner announcement is planned for next week (Please bear with us :)).

| Username            | Avg Time (ms) | Median Time (ms) | Language | Humanized Time                   |
| ------------------- | ------------- | ---------------- | -------- | -------------------------------- |
| superma-C           | 9067          | 3                | cpp      | 9.07 sec (~ 0 min 09.06 sec)     |
| k34n4y138           | 11260         | 8114             | cpp      | 11.26 sec (~ 0 min 11.25 sec)    |
| anassajaanan        | 12294         | 9904             | c        | 12.29 sec (~ 0 min 12.29 sec)    |
| ibrataha8           | 21505         | 17443            | cpp      | 21.50 sec (~ 0 min 21.50 sec)    |
| viierr              | 27806         | 17091            | cpp      | 27.81 sec (~ 0 min 27.80 sec)    |
| ouakki              | 29182         | 28706            | go       | 29.18 sec (~ 0 min 29.18 sec)    |
| yousfiSaad          | 30134         | 29761            | cpp      | 30.13 sec (~ 0 min 30.13 sec)    |
| ILKAY-BRAHIM        | 31368         | 23004            | cpp      | 31.37 sec (~ 0 min 31.36 sec)    |
| sqrt-minus-one      | 35369         | 34040            | cpp      | 35.37 sec (~ 0 min 35.36 sec)    |
| dauom               | 56548         | 57044            | cpp      | 56.55 sec (~ 0 min 56.54 sec)    |
| Hamdane-yassine     | 58869         | 60488            | rs       | 58.87 sec (~ 0 min 58.86 sec)    |
| webNeat             | 61003         | 57203            | cpp      | 61.00 sec (~ 1 min 01.00 sec)    |
| youssefhamdane      | 63224         | 69065            | rs       | 63.22 sec (~ 1 min 03.22 sec)    |
| samir-              | 63933         | 67410            | cpp      | 63.93 sec (~ 1 min 03.93 sec)    |
| essmehdi            | 68107         | 62067            | rs       | 68.11 sec (~ 1 min 08.11 sec)    |
| samir               | 68946         | 70297            | cpp      | 68.95 sec (~ 1 min 08.95 sec)    |
| ismail-bertalfilali | 82966         | 82673            | java     | 82.97 sec (~ 1 min 22.97 sec)    |
| NotAsheraf          | 99730         | 97091            | rs       | 99.73 sec (~ 1 min 39.73 sec)    |
| imOphen             | 222679        | 213030           | py       | 222.7 sec (~ 3 min 42.7 sec)     |
| j-mounim            | 243495        | 238397           | js       | 243.5 sec (~ 4 min 03.5 sec)     |
| mohammedfatihX      | 249995        | 252203           | java     | 249.99 sec (~ 4 min 09.99 sec)   |
| nizarbenalla        | 275221        | 269733           | java     | 275.22 sec (~ 4 min 35.22 sec)   |
| hamzanaciri99       | 275966        | 267462           | java     | 275.97 sec (~ 4 min 35.97 sec)   |
| lhousaine           | 299057        | 292420           | js       | 299.06 sec (~ 4 min 59.06 sec)   |
| houcine7            | 309110        | 309692           | java     | 309.11 sec (~ 5 min 09.11 sec)   |
| Moohaa              | 356836        | 359523           | java     | 356.84 sec (~ 5 min 56.84 sec)   |
| aboullaite          | 368758        | 368167           | java     | 368.76 sec (~ 6 min 08.76 sec)   |
| guizo792            | 526844        | 550883           | java     | 526.84 sec (~ 8 min 46.84 sec)   |
| YassineOsip         | 561658        | 569382           | py       | 561.66 sec (~ 9 min 21.66 sec)   |
| Smartdev110         | 776007        | 792428           | java     | 776.01 sec (~ 12 min 56.01 sec)  |
| sm3xy               | 903274        | 900806           | py       | 903.27 sec (~ 15 min 03.27 sec)  |
| ismailtlem          | 1006454       | 1005366          | py       | 1006.45 sec (~ 16 min 46.45 sec) |

We only list solutions with correct results that run within ~15 min. We unfortunately can't give feedback on every submission and we invite you to read the full assignment.

## Prize

The (one) winner will win 6000 MAD (or a PS5 :D). The winner needs to have a valid [blalaconf ticket](https://blablaconf.com/ticket) and can receive the prize in Morocco. International transfer or shipping isn't possible.
The winner will be announced on the last day of BlaBlaConf 2024. Stay tuned for more information. Make sure to check the live event on [YouTube](https://www.youtube.com/watch?v=-peiFexaSWg).

## I have a question

If you have any questions, feel free to add them to the [Discussions](https://github.com/geeksblabla/blanat/discussions/categories/q-a), and our team and contributors will be happy to help you.
