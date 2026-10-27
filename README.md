# Edge–Cloud Task Scheduling in a Shared-Network Simulation
# Планирование задач в общей сети «периферия—облако»

**Repository:** `pdp2027-edge-cloud-simulation`  
**Author / Автор:** Сулейманов Руслан Раминович (Ruslan Raminovich Suleymanov)  
**Copyright holder / Правообладатель:** Сулейманов Руслан Раминович (Ruslan Raminovich Suleymanov)  
**Status / статус:** submitted to PDP 2027 for review; not yet accepted / отправлена на рецензирование в PDP 2027; принятие пока не подтверждено.

> **Double-blind review notice / Важно о слепом рецензировании.** The English manuscript PDF is anonymized, but this public repository is associated with the personal GitHub account `@ruslansuleiman` and names the copyright holder. This may reveal the author during PDP 2027 review. / PDF рукописи анонимизирован, однако публичный репозиторий связан с личным аккаунтом GitHub `@ruslansuleiman` и указывает правообладателя. Это может раскрыть автора во время рецензирования PDP 2027.

## Русский

### О статье

Работа сравнивает циклическое распределение задач Round-Robin (RR) и жадную эвристику Shortest Estimated Completion (SEC), которая оценивает время завершения задачи на каждом узле. Исследование проводится в дискретно-событийной модели с двумя узлами периферии, одним облачным узлом, общими полнодуплексными каналами доступа, каналами отдельных узлов, очередями и многопроцессорными пулами ЦП.

При интенсивности 2 задачи/с устойчивого преимущества RR или SEC не обнаружено. При 6 задачах/с SEC снижает среднюю задержку на 75,9–76,9% относительно RR в сценариях с общим каналом 100 Мбит/с. Однако дополнительная проверка показывает, что статический взвешенный Round-Robin (WRR) в испытанных сценариях работает не хуже SEC. Поэтому вывод статьи ограничен: циклическое распределение, игнорирующее неодинаковую мощность ресурсов, может проигрывать под нагрузкой в заданной модели; превосходство SEC над всеми альтернативами не утверждается.

Параметры узлов и потоки задач синтетические, не откалиброваны на реальных измерениях и трассах. Результаты описывают только эту модель и не являются прогнозом производительности реальной инфраструктуры.

### Файлы

- `article_en.pdf` — анонимизированная англоязычная версия рукописи (5 страниц).
- `article_en.tex` — исходник статьи на LaTeX.
- `article_ru.md` — полный авторский перевод статьи на русский язык.
- `edge_cloud_sim_extended.py` — основной код событийного симулятора.
- `test_edge_cloud_sim_extended.py` — регрессионные тесты модели.
- `extended_summary.csv`, `extended_paired.csv` — сводные и парные результаты основного эксперимента.
- `extended_plot.png` — график средних задержек.
- `full_reproducibility_package.zip` — код, тесты, исходные CSV-трассы и отчёты проверки.

### Воспроизведение

Распакуйте `full_reproducibility_package.zip` и следуйте `README_extended.md`. Основная команда запуска — `python3 edge_cloud_sim_extended.py`; тесты — `python3 -m unittest -v test_edge_cloud_sim_extended.py`.

## English

### About

This paper compares cyclic Round-Robin (RR) dispatch with a greedy, mean-based Shortest Estimated Completion (SEC) heuristic that estimates where a task will finish earliest. The study uses a discrete-event model with two edge nodes, one cloud node, shared full-duplex access links, node-specific links, queues, and multicore CPU pools.

At an arrival rate of 2 tasks/s, the experiment detects no consistent advantage for RR or SEC. At 6 tasks/s, SEC reduces mean latency by 75.9–76.9% relative to RR in the 100-Mbit/s shared-access scenarios. A supplementary check finds that static weighted Round-Robin (WRR) performs at least as well as SEC in the tested scenarios. The paper therefore makes a bounded claim: capacity-blind cyclic assignment can perform poorly under load in this specified model; it does not claim that SEC is superior to all alternatives.

Node parameters and task streams are synthetic and are not calibrated against measurements or real application traces. The results apply only to this model and are not predictions of performance on a real deployment.

### Files

- `article_en.pdf` — anonymized English manuscript (5 pages).
- `article_en.tex` — LaTeX source of the manuscript.
- `article_ru.md` — full author-prepared Russian translation.
- `edge_cloud_sim_extended.py` — event-driven simulator.
- `test_edge_cloud_sim_extended.py` — model regression tests.
- `extended_summary.csv`, `extended_paired.csv` — main experiment summaries and paired results.
- `extended_plot.png` — mean-latency plot.
- `full_reproducibility_package.zip` — source code, tests, raw CSV traces, and audit reports.

### Reproduction

Unzip `full_reproducibility_package.zip` and follow `README_extended.md`. Run the simulator with `python3 edge_cloud_sim_extended.py` and tests with `python3 -m unittest -v test_edge_cloud_sim_extended.py`.

## Suggested citation / Рекомендуемая ссылка

Suleymanov, Ruslan Raminovich. “Round-Robin and Estimated-Completion Scheduling in a Shared-Network Edge–Cloud Simulation.” Research manuscript, 2026. Please cite the version and access date used. / Сулейманов Руслан Раминович. «Циклическое распределение и планирование по оценке времени завершения в моделировании общей сети “периферия—облако”». Научная рукопись, 2026. При цитировании укажите использованную версию и дату доступа.

## License / Лицензия

See [`LICENSE.md`](LICENSE.md). The copyright holder retains ownership. The license allows non-commercial sharing of the unchanged paper and translation with attribution; code and simulation data remain all-rights-reserved unless separate written permission is granted. / См. [`LICENSE.md`](LICENSE.md). Правообладатель сохраняет авторские права. Лицензия разрешает некоммерчески распространять неизменённые статью и перевод с указанием автора; код и данные остаются под режимом «все права защищены», если отдельное письменное разрешение не выдано.
