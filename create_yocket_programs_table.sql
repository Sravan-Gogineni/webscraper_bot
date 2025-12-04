-- Create YocketPrograms table (standalone, no foreign keys)
create table YocketPrograms
(
    YocketProgramID        int identity
        primary key,
    UniversityID           nvarchar(max),
    UniversitySlug         nvarchar(max),
    UniversityCourseID     nvarchar(max),
    Credential             nvarchar(max),
    UniversityCourseName   nvarchar(max),
    SchoolName             nvarchar(max),
    CourseLevel            nvarchar(max),
    ConvertedTuitionFee    float,
    Duration               int,
    IsFeeWaived            bit,
    ActualTuitionFee       float,
    Level                  int,
    IsPartner              bit,
    DeadlineDates          nvarchar(max),
    DeadlineTypes          nvarchar(max),
    SourceFile             nvarchar(max),
    CreatedDate            datetime2 default getdate()
)
go

