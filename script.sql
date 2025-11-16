create table College
(
    CollegeID                  int identity
        primary key,
    CollegeName                nvarchar(max),
    CollegeSetting             nvarchar(max),
    TypeofInstitution          nvarchar(max),
    Student_Faculty            nvarchar(max),
    NumberOfCampuses           int,
    TotalFacultyAvailable      int,
    TotalProgramsAvailable     int,
    TotalStudentsEnrolled      int,
    TotalGraduatePrograms      int,
    TotalInternationalStudents smallint,
    TotalStudents              int,
    TotalUndergradMajors       int,
    CountriesRepresented       nvarchar(max)
)
go

create table Address
(
    AddressID int identity
        primary key,
    CollegeID int not null
        unique
        constraint FK_Address_College
            references College,
    Street1   nvarchar(max),
    Street2   nvarchar(max),
    County    nvarchar(max),
    City      nvarchar(max),
    State     nvarchar(max),
    Country   nvarchar(max),
    ZipCode   nvarchar(max)
)
go

create table ApplicationRequirements
(
    AppReqID              int identity
        primary key,
    CollegeID             int not null
        unique
        constraint FK_AppReq_College
            references College,
    ApplicationFees       nvarchar(max),
    TuitionFees           nvarchar(max),
    TestPolicy            nvarchar(max),
    CoursesAndGrades      nvarchar(max),
    Recommendations       nvarchar(max),
    PersonalEssay         nvarchar(max),
    WritingSample         nvarchar(max),
    AdditionalInformation nvarchar(max),
    AdditionalDeadlines   nvarchar(max)
)
go

create table ContactInformation
(
    ContactID          int identity
        primary key,
    CollegeID          int not null
        constraint FK_Contact_College
            references College,
    LogoPath           nvarchar(max),
    Phone              nvarchar(max),
    Email              nvarchar(max),
    SecondaryEmail     nvarchar(max),
    WebsiteUrl         nvarchar(max),
    AdmissionOfficeUrl nvarchar(max),
    VirtualTourUrl     nvarchar(max),
    FinancialAidUrl    nvarchar(max)
)
go

create table Department
(
    DepartmentID   int identity
        primary key,
    DepartmentName nvarchar(max),
    Description    nvarchar(max)
)
go

create table CollegeDepartment
(
    CollegeDepartmentID int identity
        primary key,
    CollegeID           int not null
        constraint FK_CollegeDept_College
            references College,
    DepartmentID        int not null
        constraint FK_CollegeDept_Department
            references Department,
    Email               nvarchar(max),
    PhoneNumber         nvarchar(max),
    PhoneType           nvarchar(max),
    AdmissionUrl        nvarchar(max),
    BuildingName        nvarchar(max),
    Street1             nvarchar(max),
    Street2             nvarchar(max),
    City                nvarchar(max),
    State               nvarchar(max),
    StateName           nvarchar(max),
    Country             nvarchar(max),
    CountryCode         nvarchar(max),
    CountryName         nvarchar(max),
    ZipCode             nvarchar(max),
    constraint UQ_College_Department
        unique (CollegeID, DepartmentID)
)
go

create table Program
(
    ProgramID         int identity
        primary key,
    ProgramName       nvarchar(max),
    Level             nvarchar(max),
    Concentration     nvarchar(max),
    Description       nvarchar(max),
    ProgramWebsiteURL nvarchar(max),
    Accreditation     nvarchar(max),
    QsWorldRanking    nvarchar(max)
)
go

create table ProgramDepartmentLink
(
    LinkID              int identity
        primary key,
    CollegeID           int not null
        constraint FK_ProgDeptLink_College
            references College,
    ProgramID           int not null
        constraint FK_ProgDeptLink_Program
            references Program,
    CollegeDepartmentID int not null
        constraint FK_ProgDeptLink_CollegeDept
            references CollegeDepartment,
    constraint UQ_Program_College_Link
        unique (CollegeID, ProgramID)
)
go

create table ProgramRequirements
(
    ProgramReqID                int identity
        primary key,
    ProgramID                   int not null
        unique
        constraint FK_ProgramReq_Program
            references Program,
    Resume                      nvarchar(max),
    StatementOfPurpose          nvarchar(max),
    GreOrGmat                   nvarchar(max),
    EnglishScore                nvarchar(max),
    Requirements                nvarchar(max),
    WritingSample               nvarchar(max),
    IsAnalyticalNotRequired     bit not null,
    IsAnalyticalOptional        bit not null,
    IsDuoLingoRequired          bit not null,
    IsELSRequired               bit not null,
    IsGMATOrGreRequired         bit not null,
    IsGMATRequired              bit not null,
    IsGreRequired               bit not null,
    IsIELTSRequired             bit not null,
    IsLSATRequired              bit not null,
    IsMATRequired               bit not null,
    IsMCATRequired              bit not null,
    IsPTERequired               bit not null,
    IsTOEFLIBRequired           bit not null,
    IsTOEFLPBTRequired          bit not null,
    IsEnglishNotRequired        bit not null,
    IsEnglishOptional           bit not null,
    IsRecommendationSystemOpted bit not null,
    IsStemProgram               bit not null,
    IsACTRequired               bit not null,
    IsSATRequired               bit not null,
    MaxFails                    nvarchar(max),
    MaxGPA                      nvarchar(max),
    MinGPA                      nvarchar(max),
    PreviousYearAcceptanceRates nvarchar(max)
)
go

create table ProgramTermDetails
(
    ProgramTermID            int identity
        primary key,
    CollegeID                int not null
        constraint FK_ProgramTerm_College
            references College,
    ProgramID                int not null
        constraint FK_ProgramTerm_Program
            references Program,
    Term                     nvarchar(50),
    LiveDate                 datetime2,
    DeadlineDate             datetime2,
    Fees                     nvarchar(max),
    AverageScholarshipAmount nvarchar(max),
    CostPerCredit            nvarchar(max),
    ScholarshipAmount        nvarchar(max),
    ScholarshipPercentage    nvarchar(max),
    ScholarshipType          nvarchar(max),
    constraint UQ_Program_College_Term
        unique (CollegeID, ProgramID, Term)
)
go

create table ProgramTestScores
(
    TestScoreID          int identity
        primary key,
    ProgramID            int not null
        unique
        constraint FK_TestScores_Program
            references Program,
    MinimumACTScore      nvarchar(max),
    MinimumDuoLingoScore nvarchar(max),
    MinimumELSScore      nvarchar(max),
    MinimumGMATScore     nvarchar(max),
    MinimumGreScore      nvarchar(max),
    MinimumIELTSScore    nvarchar(max),
    MinimumMATScore      nvarchar(max),
    MinimumMCATScore     nvarchar(max),
    MinimumPTEScore      nvarchar(max),
    MinimumSATScore      nvarchar(max),
    MinimumTOEFLScore    nvarchar(max),
    MinimumLSATScore     nvarchar(max)
)
go

create table SocialMedia
(
    SocialID     int identity
        primary key,
    CollegeID    int           not null
        constraint FK_SocialMedia_College
            references College,
    PlatformName nvarchar(100) not null,
    URL          nvarchar(max),
    constraint UQ_College_Platform
        unique (CollegeID, PlatformName)
)
go

create table StudentStatistics
(
    StatsID                   int identity
        primary key,
    CollegeID                 int not null
        unique
        constraint FK_Stats_College
            references College,
    GradAvgTuition            int,
    GradInternationalStudents int,
    GradScholarshipHigh       int,
    GradScholarshipLow        int,
    GradTotalStudents         int,
    UGAvgTuition              int,
    UGInternationalStudents   int,
    UGScholarshipHigh         int,
    UGScholarshipLow          int,
    UGTotalStudents           int
)
go


