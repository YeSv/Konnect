ARG SDK_IMAGE='mcr.microsoft.com/dotnet/sdk:7.0'
ARG RUNTIME_IMAGE='mcr.microsoft.com/dotnet/aspnet:7.0'
ARG PROJECT_NAME='Konnect.Host'
ARG PROJECT_FILE="./src/$PROJECT_NAME/$PROJECT_NAME.csproj"
ARG HOST_PORT=80

FROM $RUNTIME_IMAGE AS base
ENV ASPNETCORE_URLS=http://+:$HOST_PORT
ENV MICROSERVICE_ENVIRONMENT=${ASPNETCORE_ENVIRONMENT}
EXPOSE $HOST_PORT
WORKDIR /app

FROM $SDK_IMAGE AS build
WORKDIR /src

COPY ./src/*/*.csproj ./
RUN for file in $(ls *.csproj); do mkdir -p ./src/${file%.*}/ && mv $file ./src/${file%.*}/; done

ARG PROJECT_FILE

COPY ./src ./src

RUN dotnet build $PROJECT_FILE -p:SolutionDir=../../ -c Release --no-restore

FROM build AS publish

ARG PROJECT_FILE

RUN dotnet publish $PROJECT_FILE -c Release -o /app --no-build

FROM base AS final
COPY --from=publish /app .

ARG PROJECT_NAME
ENV EXEC_PROJECT_NAME=$PROJECT_NAME.dll

ENTRYPOINT ["dotnet", "Konnect.Host.dll"]