# Table: ibm_analytics_employees
**Dataset:** hr_employees  
**Contract Version:** 0.2.2  
**Primary Keys:** EmployeeNumber  
**PII Columns:** EmployeeNumber, Age, Gender, MaritalStatus, Education, EducationField  

| Column Name | Type | Nullable | Derived | Description | Sensitivity |
|-------------|------|---------|---------|-------------|-------------|
| EmployeeNumber | integer | False | False | Unique employee identifier. | internal |
| Age | integer | False | False | Employee age. | internal |
| Attrition | string | False | False | Whether the employee left the company. | internal |
| BusinessTravel | string | False | False | Frequency of business travel. | internal |
| Department | string | False | False | Department the employee belongs to. | internal |
| JobRole | string | False | False | Job role of employee. | internal |
| Gender | string | False | False | Gender of employee. | sensitive |
| MaritalStatus | string | False | False | Marital status. | sensitive |
| Education | integer | False | False | Education level (1–5). | internal |
| EducationField | string | False | False | Field of education. | internal |
| DistanceFromHome | integer | False | False | Distance from home to workplace. | internal |
| DailyRate | integer | False | False | Daily salary rate. | confidential |
| HourlyRate | integer | False | False | Hourly salary rate. | confidential |
| MonthlyIncome | integer | False | False | Monthly salary. | confidential |
| MonthlyRate | integer | False | False | Monthly rate. | confidential |
| PercentSalaryHike | integer | False | False | Percentage salary increase. | confidential |
| StockOptionLevel | integer | False | False | Stock option level (0–3). | confidential |
| NumCompaniesWorked | integer | False | False | Number of companies previously worked for. | internal |
| TotalWorkingYears | integer | False | False | Total years worked. | internal |
| YearsAtCompany | integer | False | False | Years at the company. | internal |
| YearsInCurrentRole | integer | False | False | Years in current role. | internal |
| YearsSinceLastPromotion | integer | False | False | Years since last promotion. | internal |
| YearsWithCurrManager | integer | False | False | Years with current manager. | internal |
| JobLevel | integer | False | False | Job level (1–5). | internal |
| JobInvolvement | integer | False | False | Job involvement level (1–4). | internal |
| JobSatisfaction | integer | False | False | Job satisfaction level (1–4). | internal |
| RelationshipSatisfaction | integer | False | False | Relationship satisfaction level (1–4). | internal |
| EnvironmentSatisfaction | integer | False | False | Satisfaction with work environment (1–4). | internal |
| WorkLifeBalance | integer | False | False | Work-life balance score (1–4). | internal |
| TrainingTimesLastYear | integer | False | False | Training sessions attended last year. | internal |
| PerformanceRating | integer | False | False | Performance rating. | internal |
| EmployeeCount | integer | False | False | Count of employees (dataset-provided value). | internal |
| StandardHours | integer | False | False | Standard working hours. | internal |
| Over18 | string | False | False | Indicates employee is over 18. | internal |
| OverTime | string | False | False | Whether the employee works overtime. | internal |
| YearsAtCompanyBucket | string | True | False | Tenure bucket grouped as 0–2, 3–5, 6+ years. |  |
| TenureRatio | double | True | False | YearsInCurrentRole divided by YearsAtCompany. |  |
| IncomePerYearAtCompany | double | True | False | MonthlyIncome divided by YearsAtCompany. |  |
| _load_id | metadata | True | False | Metadata column _load_id | internal |
| _generated_at | metadata | True | False | Metadata column _generated_at | internal |