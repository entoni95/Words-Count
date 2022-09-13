#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <stddef.h>
#include <string.h>
#include <time.h>

#include <ctype.h>
#include <math.h>

#define COLONNE 5
#define RIGHE 8


#define MASTER 0
#define BUF_SIZE 2048
#define CHAR_SIZE 900                                                       
#define CHARS 13                                                            
#define MAX_SIZE 100000
#define N_TIMES 1000

#define ABS(X)  ((X>=0)? X : -(x) )
#define ROUND(X)  (X>=0)? (int) (X + 0.5) : (int)-(ABS(X) +0.5)
#define L_BLOCK 1000

clock_t startm, stopm;
#define START if ( (startm = clock()) == -1) {printf("Error calling clock");exit(1);}
#define STOP if ( (stopm = clock()) == -1) {printf("Error calling clock");exit(1);}
#define PRINTTIME printf( "\n\n%6.3f seconds used by the processor.\n\n", ((double)stopm-startm)/CLOCKS_PER_SEC);


//start struct functions

typedef struct word_s {
    int times[N_TIMES];
    unsigned char name[][CHARS];

} word;

word *createWordArray() {
    word *words = calloc(MAX_SIZE, sizeof (words));
    return words;
}

//end struct functions

//start other functions

void str_lower(char *str) {
    for (; str[0] != '\0'; str++) {
        str[0] = (char) tolower(str[0]);
    }
}

void printArray(int *row, int nElements) {
    int i;
    for (i = 0; i < nElements; i++) {
        printf("%d ", row[i]);
    }
    printf("\n");
}

void removeChar(char *str, char garbage) {

    char *src, *dst;
    for (src = dst = str; *src != '\0'; src++) {
        *dst = *src;
        if (*dst != garbage) dst++;
    }
    *dst = '\0';
}

//end other functions

int main(int argc, char **argv) {
    
    START;

    int num_of_files = 0;
    int i = 0;
	int j = 0;
	int k = 0;
    float resto = 0;
    FILE *f = NULL;
    char f_title[10];
    char str_nfiles[10];
    int n_parole = 0;

    int righe_totali = 0;
    int cont_parole = 0; //contiene il numero delle parole totali escluse le ripetizioni
    int ripetizioni_parola = 0; //conta il numero di volte in cui una parola si ripete
    /*
     ** ASCII TABLE **
     * \0 -> null
     * 10 -> enter
     * 32 -> " "
     * 33 -> "!"
     * 39 -> "'"
     * 44 -> ","
     * 46 -> "."
     * 58 -> ":"
     * 59 -> ";"
     * 63 -> "?"
     */
    char delim[10] = {10, 32, 33, 39, 44, 46, 58, 59, 63}; //contains the characters that delimit the division of words

    char *ptr;


    char **str_righe = (char**) calloc(MAX_SIZE, sizeof (char*));
    for (i = 0; i < MAX_SIZE; i++) {
        str_righe[i] = (char*) calloc(CHAR_SIZE, sizeof (char));
    }
	
    char **parole = (char**) calloc(MAX_SIZE, sizeof (char*)); //array delle parole incluse le ripetizioni
    for (i = 0; i < MAX_SIZE; i++) {
        parole[i] = (char*) calloc(CHAR_SIZE, sizeof (char));
    }
	
    char **ptr_parole = (char**) calloc(MAX_SIZE, sizeof (char*)); //array delle parole escluse le ripetizioni
    for (i = 0; i < MAX_SIZE; i++) {
        ptr_parole[i] = (char*) calloc(CHAR_SIZE, sizeof (char));
    }

    int start = 0;
    int end = 0;

    const int tag = 0;
    int size, rank;

    int n_parole_processore = 0;
    int *matrix = 0;
    int *procRow = 0;

    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 2) {
        fprintf(stderr, "Requires at least two processes.\n");
        exit(-1);
    }

    /* create a type for struct word */
    const int nitems = 2;
    int blocklengths[2] = {L_BLOCK, L_BLOCK};
    MPI_Datatype types[2] = {MPI_INT, MPI_INT};
    MPI_Datatype MPI_WCOUNT_TYPE;
    MPI_Aint offsets[2];

    offsets[0] = offsetof(word, name);
    offsets[1] = offsetof(word, times);

    MPI_Type_create_struct(nitems, blocklengths, offsets, types, &MPI_WCOUNT_TYPE);
    MPI_Type_commit(&MPI_WCOUNT_TYPE);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (rank == MASTER) {

        MPI_Status status;
        
        sscanf(argv[1], "%d", &num_of_files);
        if ((num_of_files <= 0) || (num_of_files > 16)) {
            fprintf(stderr, "Insert a valid number of files (from 1 to 16).\n");
            exit(-1);
        }
        
        //carico i file
        for (i = 1; i <= num_of_files; i++) {
            sprintf(str_nfiles, "%d", i);
            strcpy(f_title, str_nfiles);
            strcat(f_title, ".txt");

            f = fopen(f_title, "r");
            while (fgets(str_righe[j], BUF_SIZE, f)) j++;

            fclose(f);
        }
        righe_totali = j;

        //salvo l'output all'interno di un file temporaneo
        FILE *fTemp = fopen("output.txt", "w");

        for (i = 0; i < righe_totali; ++i) {

            ptr = strtok(str_righe[i], delim);
            while (ptr != NULL) {
                str_lower(ptr);
                const char *text = ptr;
                fprintf(fTemp, "%s\n", text);
                ptr = strtok(NULL, delim);
                n_parole++;
            }
        }
        fclose(fTemp);
        //se il numero delle parole è dispari
        resto = n_parole % 2;
        if (resto != 0) {
            int x = ROUND(resto / size);
            n_parole = n_parole + x;
        }

        //invio il numero delle parole agli slaves
        MPI_Bcast(
                /* data         = */ &n_parole,
                /* count        = */ 1,
                /* datatype     = */ MPI_INT,
                /* root         = */ MASTER,
                /* communicator = */ MPI_COMM_WORLD);
				
        printf("Rank %d: n_parole %d\n", rank, n_parole);
        //start scatter
        matrix = malloc(sizeof (int) * n_parole);
        j = 1;
        for (i = 0; i <= n_parole; i++) {
            matrix[i] = j;
            j++;
        }

        n_parole_processore = n_parole / (size);
        procRow = malloc(sizeof (int) * n_parole); // received row will contain p integers

        MPI_Scatter(
                /* send_data       = */ matrix,
                /* send_count      = */ n_parole_processore,
                /* send_datatype   = */ MPI_INT,
                /* recv_data       = */ procRow,
                /* recv_count      = */ n_parole_processore,
                /* recv_datatype   = */ MPI_INT,
                /* root            = */ MASTER,
                /* MPI_commuicator = */ MPI_COMM_WORLD);
        //end scatter    
        //printf("Rank %d: start: %d, end: %d\n", rank, procRow[0], procRow[n_parole_processore - 1]);
        word *globalWords = createWordArray();
        //start
        FILE* file = fopen("output.txt", "r");
        char line[256]; //todo da vedere se c'è bisogno di calloc
        i = 0;
        j = 0;
        start = procRow[0];
        end = procRow[n_parole_processore - 1];
        printf("Rank %d: start: %d, end: %d\n", rank, start, end);

        word *send = createWordArray();
        word *recv = createWordArray();
		
        while (fgets(line, sizeof (line), file)) {
            i++;
            for (j = start; j <= end; j++) {
                if (i == j) {
                    strcpy(parole[i], line);
                    removeChar(parole[i], '\n');
                }
            }
        }
        fclose(file);

        for (i = 0; i <= end; i++) {
            for (j = 0; j <= end; j++) {
                if (i == j) {
                    strcpy(ptr_parole[k], parole[i]);
                    k++;
                    cont_parole++;
                    break;
                } else {
                    if (strcasecmp(ptr_parole[j], parole[i]) != 0)
                        continue;
                    else
                        break;
                }
            }
        }

        k = -1; //struct counter
        for (i = 0; i < cont_parole; i++) {
            for (j = 0; j <= end; j++) {
                if (strcasecmp(ptr_parole[i], parole[j]) == 0)
                    ripetizioni_parola++;
            }
            memcpy(send->name[k], (unsigned char *)ptr_parole[i], sizeof(ptr_parole[i]));
            send->times[k] = ripetizioni_parola;
            //printf("Rank %d: k=%d %s -> %dv\n", rank, k, send->name[k], send->times[k]);
            ripetizioni_parola = 0;
            k++;
        }

        //const int dest = MASTER;
        //MPI_Send(send, sizeof (send), MPI_WCOUNT_TYPE, dest, tag, MPI_COMM_WORLD);

        int i = 0, j = 0;
        int sem = 0;

        while (memcmp(send->name[i], (unsigned char *)"", sizeof(""))) {
            while (memcmp(globalWords->name[j], (unsigned char *)"", sizeof(""))) {
                if (memcmp(send->name[i], (unsigned char *)globalWords->name[j], sizeof(globalWords->name[j])) == 0) {
                    globalWords->times[j] = globalWords->times[j] + send->times[i];
                    sem = 1;
                    j++;
                } else {
                    j++;
                }
            }
            if (sem == 0) {
                memcpy(globalWords->name[j], (unsigned char *)send->name[i], sizeof(send->name[i]));
                globalWords->times[j] = send->times[i];
            }
            i++;
            j = 0;
            sem = 0;
        }

        //free(send);
        //end  
		for (int slave = 1; slave < size; slave++) {
			MPI_Recv(recv, sizeof (recv), MPI_WCOUNT_TYPE, slave, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			
        i = 0;
        j = 0;
        sem = 0;

        while (memcmp(recv->name[i], (unsigned char *)"", sizeof(""))) {
            while (memcmp(globalWords->name[j], (unsigned char *)"", sizeof(""))) {
                if (memcmp(recv->name[i], (unsigned char *)globalWords->name[j], sizeof(globalWords->name[j])) == 0) {
                    globalWords->times[j] = globalWords->times[j] + recv->times[i];
                    sem = 1;
                    j++;
                } else {
                    j++;
                }
            }
            if (sem == 0) {
                memcpy(globalWords->name[j], (unsigned char *)recv->name[i], sizeof(recv->name[i]));
                globalWords->times[j] = recv->times[i];
            }
            i++;
            j = 0;
            sem = 0;
        }
		}
        i = 0;
        while (memcmp(globalWords->name[i], (unsigned char *)"", sizeof(""))) {
            printf("Rank %d: Received: %s -> %d times\n", rank,
                    globalWords->name[i], globalWords->times[i]);
            i++;
        }

		//free(send);
        //free(recv);
        //free(globalWords);
        
        STOP;
        PRINTTIME;
        
    } else {
        //ricevo il numero delle parole dal master
        MPI_Bcast(
                /* data         = */ &n_parole,
                /* count        = */ 1,
                /* datatype     = */ MPI_INT,
                /* root         = */ MASTER,
                /* communicator = */ MPI_COMM_WORLD);
        
        FILE* file = fopen("output.txt", "r");
        char line[256]; //todo da vedere se c'è bisogno di calloc
        //start scatter
        matrix = malloc(sizeof (int) * n_parole);
        j = 1;
        for (i = 0; i <= n_parole; i++) {
            matrix[i] = j;
            j++;
        }
        n_parole_processore = n_parole / (size); //master not included
        procRow = malloc(sizeof (int) * n_parole); // received row will contain p integers

        MPI_Scatter(matrix, n_parole_processore, MPI_INT, procRow, n_parole_processore, MPI_INT, MASTER, MPI_COMM_WORLD);
        //end scatter
        i = 0;
        j = 0;
        start = procRow[0];
        end = procRow[n_parole_processore - 1];
        printf("Rank %d: start: %d, end: %d\n", rank, start, end);
        word *send = createWordArray();
        while (fgets(line, sizeof (line), file)) {
            i++;
            for (j = start; j <= end; j++) {
                if (i == j) {
                    strcpy(parole[i], line);
                    removeChar(parole[i], '\n');
                }
            }
        }
        fclose(file);

        for (i = 0; i <= end; i++) {
            for (j = 0; j <= end; j++) {
                if (i == j) {
                    strcpy(ptr_parole[k], parole[i]);
                    k++;
                    cont_parole++;
                    break;
                } else {
                    if (strcasecmp(ptr_parole[j], parole[i]) != 0)
                        continue;
                    else
                        break;
                }
            }
        }
        k = -1; //struct counter
        for (i = 0; i < cont_parole; i++) {
            for (j = 0; j <= end; j++) {
                if (strcasecmp(ptr_parole[i], parole[j]) == 0)
                    ripetizioni_parola++;
            }
            memcpy(send->name[k], (unsigned char *)ptr_parole[i], sizeof(ptr_parole[i]));
            send->times[k] = ripetizioni_parola;
            //printf("Rank %d: k=%d %s -> %dv\n", rank, k, send->name[k], send->times[k]);
            ripetizioni_parola = 0;
            k++;
        }

        MPI_Send(send, sizeof (send), MPI_WCOUNT_TYPE, MASTER, tag, MPI_COMM_WORLD);

        printf("Rank %d: sent structure word\n", rank);
        //free(send);
    }

	
		
	//free(parole);
    //free(ptr_parole);
    //free(matrix);
    //free(procRow);
	
    MPI_Type_free(&MPI_WCOUNT_TYPE);
    MPI_Finalize();
    
    return 0;
}
