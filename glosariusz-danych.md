# Glosariusz danych (`Transaction history*.csv` i `stock-sales*.txt`)

Dokument opisuje znaczenie pól oraz to, jak dane są **faktycznie interpretowane przez kod** projektu.

## 1) `Transaction history*.csv`

## 1.1 Kolumny wejściowe

- `Transaction date`
  - Data w formacie `Mon-DD-YYYY` (np. `Sep-13-2024`).
  - Parsowana do `trade_date`.
- `Transaction type`
  - Typ operacji (np. `YOU SOLD`, `DIVIDEND RECEIVED`).
  - Kod obcina wszystko po `;` (zostaje część przed średnikiem).
- `Investment name`
  - Nazwa instrumentu/funduszu.
- `Shares`
  - Liczba akcji/jednostek.
  - Dla `YOU BOUGHT` zwykle dodatnia, dla `YOU SOLD` ujemna.
  - `-` oznacza brak zastosowania (parsuje się do pustej wartości liczbowej).
- `Amount`
  - Kwota USD (np. `$3,919.78`, `-$1.31`).
  - Kod usuwa `$` i `,`, następnie parsuje do `amount_usd`.

## 1.2 Symbole i zapisy spotykane w eksporcie

- `$` -> kwota w USD.
- `-` przed kwotą -> ujemny przepływ gotówkowy.
- `-` jako cała wartość pola (`Shares`, czasem `Investment name`) -> brak wartości.
- `###` / `####` (np. `YOU BOUGHT ESPP###`, `YOU BOUGHT RSU####`) -> zamaskowane identyfikatory planu.
- `AS OF` (np. `YOU BOUGHT ESPP### AS OF 09-13-24`) -> data referencyjna w opisie operacji.

## 1.3 Typowe `Transaction type` i ich rola

## 1.3.1 Wykorzystywane bezpośrednio w kalkulacji

- `YOU SOLD`
  - Sprzedaż akcji (przychód w części C/D PIT-38).
- `YOU BOUGHT ...`
  - Zakup/nabycie (koszt w FIFO).
- `YOU BOUGHT RSU####`
  - Nabycie RSU; w `custom` służy do dopasowania kursu nabycia przy przeliczeniu `Cost basis` do PLN.
  - Jeśli `Cost basis` jest puste/niepoprawne i `Stock source=RS`, kod używa fallbacku `0.0` (z ostrzeżeniem).
- `YOU BOUGHT ESPP### AS OF ...`
  - Nabycie ESPP; w `custom` służy do dopasowania kursu nabycia przy przeliczeniu `Cost basis` do PLN.
  - Jeśli `Cost basis` jest puste/niepoprawne i `Stock source=SP`, kod używa fallbacku z pasującego zakupu ESPP (z ostrzeżeniem).
- `DIVIDEND RECEIVED`
  - Wypłata ujmowana w Części G (art. 30a); kod rozróżnia ją pomocniczo na:
  - `equity-like` (np. akcje),
  - `fund-like` (np. fundusze/MMF/cash sweep), na podstawie `Investment name`.
- `REINVESTMENT ...`
  - Reinvest (zakup jednostek za wypłatę); kod nie dolicza tych rekordów do podstawy podatku Części G.
- `NON-RESIDENT TAX ...`
  - Kod rozdziela:
  - wpisy z `...DIVIDEND...` -> podatek zagraniczny do Części G (poz. 46),
  - wpisy bez kontekstu `DIVIDEND/REINVESTMENT` -> podatek zagraniczny do art. 30b (poz. 32).

## 1.3.2 Najczęściej pomocnicze (nie tworzą same z siebie zysku/straty z akcji)

- `JOURNALED CASH WITHDRAWAL`
- `JOURNALED WIRE/CHECK FEE`
- `JOURNALED SPP PURCHASE CREDIT`
- `JOURNALED ...` (inne warianty)
- `TRANSFERRED ...`
- `EXCHANGED ...`

## 1.4 Reguły parsera (ważne dla jakości danych)

- Przy wielu plikach CSV dane są łączone bez automatycznej deduplikacji.
- Jeśli identyczne rekordy pojawią się w różnych plikach wejściowych, kod zgłasza błąd (`ValueError`) i wymaga usunięcia nakładania danych.
- Niepoprawny format daty daje `NaT`; rekordy bez `settlement_date` są pomijane później, a pipeline loguje ostrzeżenie z liczbą odrzuconych wierszy.
- Rok podatkowy filtrowany jest po `settlement_date` (nie po `Transaction date`).

## 2) `stock-sales*.txt` (tryb `--method custom`)

Plik jest TSV (separator tabulacji), zwykle z widoku Fidelity „Stock Sales”.

## 2.1 Kolumny wejściowe

- `Date sold or transferred`
  - Data sprzedaży lotu (`Date sold` po parsowaniu).
- `Date acquired`
  - Data nabycia lotu.
- `Quantity`
  - Liczba akcji dla lotu.
- `Cost basis`
  - Podstawowe źródło kosztu lotu w `custom`.
  - Kiedy da się sparsować kwotę USD, kod przelicza ją na PLN po kursie z dopasowanego nabycia (`Date acquired`).
  - Jeśli brak/niepoprawna wartość, kod używa fallbacków zależnych od `Stock source`.
- `Proceeds`
  - Kolumna informacyjna; kod jej nie używa bezpośrednio.
- `Gain/loss`
  - Kolumna informacyjna; kod jej nie używa bezpośrednio.
- `Stock source`
  - Steruje fallbackiem, gdy nie ma poprawnego `Cost basis`:
  - `RS` -> fallback kosztu lotu `0.0`.
  - `SP` -> fallback kosztu z pasującego zakupu ESPP.
  - inne wartości -> fallback kosztu z pasującego zakupu `YOU BOUGHT` (bez filtra ESPP).
- `Symbol` (opcjonalnie)
  - Jeśli występuje, kod używa go do zawężenia dopasowania transakcji przy tej samej dacie sprzedaży.
  - Najpierw próbuje dopasowanie po kolumnie `Symbol` (jeśli istnieje w danych transakcyjnych), a następnie po `Investment name`.

## 2.2 Jak działa dopasowanie w `custom` (istotne ograniczenia)

- Sprzedaż jest wyszukiwana najpierw po `trade_date`, potem po `settlement_date`.
- Zakup jest wyszukiwany najpierw po `trade_date`, potem po `settlement_date`.
- Jeśli w wierszu custom podano `Symbol`, dopasowanie sprzedaży jest dodatkowo zawężane po tym symbolu.
- Jeśli dla danej daty jest wiele pasujących rekordów sprzedaży/zakupu, kod bierze `iloc[0]` (pierwszy pasujący rekord).
- W praktyce oznacza to, że przy wielu transakcjach tego samego dnia warto mieć dane tak przygotowane, by nie było niejednoznaczności ceny/lotu.

## 3) Spójność danych, którą warto utrzymywać

- Suma `Quantity` w `stock-sales*.txt` dla daty sprzedaży powinna odpowiadać liczbie akcji sprzedanych (`YOU SOLD`) dla tej daty.
- Dla `Stock source=SP` `Date acquired` powinno wskazywać na istniejący zakup ESPP.
- Dla `Stock source=RS` `Date acquired` powinno wskazywać na istniejące nabycie RSU.
- Łącznie nie powinno się sprzedać więcej akcji niż wcześniej nabyto/otrzymano.
